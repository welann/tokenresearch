use std::error::Error;
use std::fmt::Write as _;

use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use tokenresearch::model::{BookView, CollectorHealth, GapWindow, MarketRef, Venue};
use tokenresearch::query::{QueryStore, SnapshotMeta, TimeRange};
use tokenresearch::storage::SqliteBookStore;

#[derive(Parser, Debug)]
#[command(name = "query")]
#[command(about = "Simple query CLI for tokenresearch SQLite data")]
struct Cli {
    #[arg(long, default_value = "tokenresearch.sqlite")]
    db: String,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let cli = Cli::parse();
    let store = SqliteBookStore::connect(&cli.db).await?;
    let query = QueryStore::new(store);

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
