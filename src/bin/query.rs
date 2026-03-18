use std::error::Error;
use std::fmt::Write as _;

use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use tokenresearch::model::{BookView, CollectorHealth, GapWindow, MarketRef, Venue};
use tokenresearch::price_model::{
    LatestPrice, PriceGapWindow, PriceHealth, PriceKind, PriceMarket, PriceRangeRequest,
    PriceResolution, PriceSeries,
};
use tokenresearch::price_query::{PriceQueryStore, TimeRange as PriceTimeRange};
use tokenresearch::price_storage::SqlitePriceStore;
use tokenresearch::query::{QueryStore, SnapshotMeta, TimeRange};
use tokenresearch::storage::SqliteBookStore;

#[derive(Parser, Debug)]
#[command(name = "query")]
#[command(about = "Simple query CLI for tokenresearch SQLite data")]
struct Cli {
    #[arg(long, default_value = "tokenresearch.sqlite")]
    db: String,
    #[arg(long, default_value = "token_prices.sqlite")]
    price_db: String,
    #[arg(long)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Markets {
        #[arg(long)]
        venue: Option<Venue>,
    },
    Latest(BookArgs),
    BookAt(BookAtArgs),
    Events(RangedArgs),
    Snapshots(RangedArgs),
    Gaps(GapArgs),
    Health(MarketArgs),
    PriceMarkets {
        #[arg(long)]
        venue: Option<Venue>,
    },
    PriceLatest(PriceLatestArgs),
    PriceRange(PriceRangeArgs),
    PriceGaps(PriceGapArgs),
    PriceHealth(PriceHealthArgs),
}

#[derive(Args, Debug)]
struct BookArgs {
    #[arg(long)]
    venue: Venue,
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value_t = 10)]
    depth: usize,
}

#[derive(Args, Debug)]
struct MarketArgs {
    #[arg(long)]
    venue: Venue,
    #[arg(long)]
    symbol: String,
}

#[derive(Args, Debug)]
struct BookAtArgs {
    #[arg(long)]
    venue: Venue,
    #[arg(long)]
    symbol: String,
    #[arg(long)]
    ts_ms: i64,
    #[arg(long, default_value_t = 10)]
    depth: usize,
}

#[derive(Args, Debug)]
struct RangedArgs {
    #[arg(long)]
    venue: Venue,
    #[arg(long)]
    symbol: String,
    #[arg(long)]
    start_ms: Option<i64>,
    #[arg(long)]
    end_ms: Option<i64>,
    #[arg(long, default_value_t = 50)]
    limit: usize,
}

#[derive(Args, Debug)]
struct GapArgs {
    #[arg(long)]
    venue: Venue,
    #[arg(long)]
    symbol: String,
    #[arg(long)]
    start_ms: Option<i64>,
    #[arg(long)]
    end_ms: Option<i64>,
}

#[derive(Args, Debug)]
struct PriceLatestArgs {
    #[arg(long)]
    token: String,
    #[arg(long)]
    kind: PriceKind,
    #[arg(long)]
    venue: Option<Venue>,
    #[arg(long)]
    symbol: Option<String>,
}

#[derive(Args, Debug)]
struct PriceRangeArgs {
    #[arg(long)]
    token: Option<String>,
    #[arg(long)]
    venue: Option<Venue>,
    #[arg(long)]
    symbol: Option<String>,
    #[arg(long)]
    kind: PriceKind,
    #[arg(long)]
    start_ms: i64,
    #[arg(long)]
    end_ms: i64,
    #[arg(long, default_value = "auto")]
    resolution: PriceResolution,
}

#[derive(Args, Debug)]
struct PriceGapArgs {
    #[arg(long)]
    token: String,
    #[arg(long)]
    venue: Option<Venue>,
    #[arg(long)]
    start_ms: Option<i64>,
    #[arg(long)]
    end_ms: Option<i64>,
}

#[derive(Args, Debug)]
struct PriceHealthArgs {
    #[arg(long)]
    venue: Venue,
    #[arg(long)]
    symbol: String,
    #[arg(long)]
    kind: PriceKind,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let cli = Cli::parse();
    let store = SqliteBookStore::connect(&cli.db).await?;
    let query = QueryStore::new(store);
    let price_store = SqlitePriceStore::connect(&cli.price_db).await?;
    let price_query = PriceQueryStore::new(price_store);

    match cli.command {
        Command::Markets { venue } => {
            let markets = query.list_markets(venue).await?;
            emit(&markets, cli.json, render_markets(&markets))?;
        }
        Command::Latest(args) => {
            let market = market_ref(&args.venue, &args.symbol);
            let book = query
                .latest_book(&market, args.depth)
                .await?
                .ok_or_else(|| {
                    format!("latest book not found for {} {}", args.venue, args.symbol)
                })?;
            emit(&book, cli.json, render_book(&book))?;
        }
        Command::BookAt(args) => {
            let market = market_ref(&args.venue, &args.symbol);
            let book = query.book_at(&market, args.ts_ms, args.depth).await?;
            emit(&book, cli.json, render_book(&book))?;
        }
        Command::Events(args) => {
            let market = market_ref(&args.venue, &args.symbol);
            let events = query
                .events(&market, range(args.start_ms, args.end_ms), args.limit)
                .await?;
            emit(&events, cli.json, render_events(&events))?;
        }
        Command::Snapshots(args) => {
            let market = market_ref(&args.venue, &args.symbol);
            let snapshots = query
                .snapshots(&market, range(args.start_ms, args.end_ms), args.limit)
                .await?;
            emit(&snapshots, cli.json, render_snapshots(&snapshots))?;
        }
        Command::Gaps(args) => {
            let market = market_ref(&args.venue, &args.symbol);
            let gaps = query
                .gaps(&market, range(args.start_ms, args.end_ms))
                .await?;
            emit(&gaps, cli.json, render_gaps(&gaps))?;
        }
        Command::Health(args) => {
            let market = market_ref(&args.venue, &args.symbol);
            let health = query.collector_state(&market).await?.ok_or_else(|| {
                format!(
                    "collector state not found for {} {}",
                    args.venue, args.symbol
                )
            })?;
            emit(&health, cli.json, render_health(&health))?;
        }
        Command::PriceMarkets { venue } => {
            let markets = price_query.list_price_markets(venue).await?;
            emit(&markets, cli.json, render_price_markets(&markets))?;
        }
        Command::PriceLatest(args) => {
            let latest = price_query
                .latest_price(&args.token, args.kind, args.venue, args.symbol.as_deref())
                .await?;
            emit(&latest, cli.json, render_latest_prices(&latest))?;
        }
        Command::PriceRange(args) => {
            let series = price_query
                .price_range(PriceRangeRequest {
                    token: args.token,
                    venue: args.venue,
                    market_symbol: args.symbol,
                    kind: args.kind,
                    start_ms: args.start_ms,
                    end_ms: args.end_ms,
                    resolution: args.resolution,
                })
                .await?;
            emit(&series, cli.json, render_price_series(&series))?;
        }
        Command::PriceGaps(args) => {
            let gaps = price_query
                .price_gaps(
                    &args.token,
                    args.venue,
                    PriceTimeRange {
                        start_ms: args.start_ms,
                        end_ms: args.end_ms,
                    },
                )
                .await?;
            emit(&gaps, cli.json, render_price_gaps(&gaps))?;
        }
        Command::PriceHealth(args) => {
            let health = price_query
                .price_health(args.venue, &args.symbol, args.kind)
                .await?
                .ok_or_else(|| {
                    format!(
                        "price health not found for {} {} {}",
                        args.venue, args.symbol, args.kind
                    )
                })?;
            emit(&health, cli.json, render_price_health(&health))?;
        }
    }

    Ok(())
}

fn market_ref(venue: &Venue, symbol: &str) -> MarketRef {
    MarketRef::new(*venue, symbol)
}

fn range(start_ms: Option<i64>, end_ms: Option<i64>) -> TimeRange {
    TimeRange { start_ms, end_ms }
}

fn emit<T>(value: &T, json: bool, text: String) -> Result<(), Box<dyn Error + Send + Sync>>
where
    T: Serialize,
{
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        println!("{text}");
    }
    Ok(())
}

fn render_markets(markets: &[tokenresearch::NormalizedMarket]) -> String {
    let mut output = String::from("venue\tsymbol\tstatus\tbase\tquote\tmarket_id\n");
    for market in markets {
        let _ = writeln!(
            output,
            "{}\t{}\t{:?}\t{}\t{}\t{}",
            market.market.venue,
            market.market.symbol,
            market.status,
            market.base_asset,
            market.quote_asset,
            market.venue_market_id
        );
    }
    output.trim_end().to_string()
}

fn render_book(book: &BookView) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "market\t{}\t{}",
        book.market.venue, book.market.symbol
    );
    let _ = writeln!(
        output,
        "exchange_ts_ms\t{}",
        book.exchange_ts_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string())
    );
    let _ = writeln!(output, "received_ts_ms\t{}", book.received_ts_ms);
    output.push_str("bids\n");
    for level in &book.bids {
        let _ = writeln!(output, "{}\t{}", level.price, level.quantity);
    }
    output.push_str("asks\n");
    for level in &book.asks {
        let _ = writeln!(output, "{}\t{}", level.price, level.quantity);
    }
    output.trim_end().to_string()
}

fn render_events(events: &[tokenresearch::NormalizedBookEvent]) -> String {
    let mut output = String::from("received_ts_ms\tkind\tsequence\tbids\tasks\n");
    for event in events {
        let sequence = event
            .sequence
            .as_ref()
            .map(|sequence| format!("{}..{}", sequence.start, sequence.end))
            .unwrap_or_else(|| "-".to_string());
        let _ = writeln!(
            output,
            "{}\t{:?}\t{}\t{}\t{}",
            event.received_ts_ms,
            event.kind,
            sequence,
            event.bids.len(),
            event.asks.len()
        );
    }
    output.trim_end().to_string()
}

fn render_snapshots(snapshots: &[SnapshotMeta]) -> String {
    let mut output = String::from("id\tvenue\tsymbol\tcreated_at_ms\tdepth\n");
    for snapshot in snapshots {
        let _ = writeln!(
            output,
            "{}\t{}\t{}\t{}\t{}",
            snapshot.id,
            snapshot.market.venue,
            snapshot.market.symbol,
            snapshot.created_at_ms,
            snapshot.depth
        );
    }
    output.trim_end().to_string()
}

fn render_gaps(gaps: &[GapWindow]) -> String {
    let mut output = String::from("started_at_ms\tended_at_ms\texpected\tobserved\treason\n");
    for gap in gaps {
        let _ = writeln!(
            output,
            "{}\t{}\t{}\t{}\t{}",
            gap.started_at_ms,
            gap.ended_at_ms,
            gap.expected_sequence
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            gap.observed_sequence
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            gap.reason
        );
    }
    output.trim_end().to_string()
}

fn render_health(health: &CollectorHealth) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "market\t{}\t{}",
        health.market.venue, health.market.symbol
    );
    let _ = writeln!(output, "status\t{}", health.status);
    let _ = writeln!(output, "updated_at_ms\t{}", health.updated_at_ms);
    let _ = writeln!(
        output,
        "last_sequence_end\t{}",
        health
            .last_sequence_end
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    let _ = writeln!(
        output,
        "last_gap_at_ms\t{}",
        health
            .last_gap_at_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    output.trim_end().to_string()
}

fn render_price_markets(markets: &[PriceMarket]) -> String {
    let mut output =
        String::from("venue\tsymbol\ttoken\tquote\tstatus\ttrade_history\treference_history\n");
    for market in markets {
        let _ = writeln!(
            output,
            "{}\t{}\t{}\t{}\t{:?}\t{}\t{}",
            market.market.venue,
            market.market.symbol,
            market.token,
            market.quote_asset,
            market.status,
            market.supports_trade_history,
            market.supports_reference_history
        );
    }
    output.trim_end().to_string()
}

fn render_latest_prices(prices: &[LatestPrice]) -> String {
    let mut output = String::from("venue\tsymbol\ttoken\tkind\tresolution\tts_ms\tclose\n");
    for price in prices {
        let _ = writeln!(
            output,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            price.venue,
            price.market_symbol,
            price.token,
            price.kind,
            price.resolution,
            price.ts_ms,
            price.close
        );
    }
    output.trim_end().to_string()
}

fn render_price_series(series: &[PriceSeries]) -> String {
    let mut output = String::from("venue\tsymbol\ttoken\tkind\tresolution\tpoints\n");
    for entry in series {
        let _ = writeln!(
            output,
            "{}\t{}\t{}\t{}\t{}\t{}",
            entry.venue,
            entry.market_symbol,
            entry.token,
            entry.kind,
            entry.resolution,
            entry.points.len()
        );
    }
    output.trim_end().to_string()
}

fn render_price_gaps(gaps: &[PriceGapWindow]) -> String {
    let mut output =
        String::from("venue\tsymbol\tkind\tresolution\tstarted_at_ms\tended_at_ms\treason\n");
    for gap in gaps {
        let _ = writeln!(
            output,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            gap.market.venue,
            gap.market.symbol,
            gap.kind,
            gap.resolution,
            gap.started_at_ms,
            gap.ended_at_ms,
            gap.reason
        );
    }
    output.trim_end().to_string()
}

fn render_price_health(health: &PriceHealth) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "market\t{}\t{}\t{}",
        health.market.venue, health.market.symbol, health.kind
    );
    let _ = writeln!(output, "status\t{}", health.status);
    let _ = writeln!(output, "updated_at_ms\t{}", health.updated_at_ms);
    let _ = writeln!(
        output,
        "last_live_bucket_ms\t{}",
        health
            .last_live_bucket_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    let _ = writeln!(
        output,
        "last_candle_open_ms\t{}",
        health
            .last_candle_open_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    let _ = writeln!(
        output,
        "last_backfill_open_ms\t{}",
        health
            .last_backfill_open_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    let _ = writeln!(
        output,
        "last_gap_at_ms\t{}",
        health
            .last_gap_at_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    output.trim_end().to_string()
}
