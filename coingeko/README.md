# CoinGecko Metrics

`coingeko/` 现在包含两部分能力：

- `coingecko_top300_daily.py`：抓取 Top300 原始数据
- `metrics/`：基于日频 CSV 计算研究指标
- `webapp/`：静态 React 可视化前端

推荐流程：

```bash
cd coingeko
uv run python -m metrics.cli prepare \
  --universe-csv coingecko_out/universe_top300_usd_days365.csv \
  --daily-csv coingecko_out/market_chart_daily_top300_usd_days365.csv \
  --out-dir analysis_out

uv run python -m metrics.cli correlation --out-dir analysis_out
uv run python -m metrics.cli cointegration --out-dir analysis_out
uv run python -m metrics.cli market-model --out-dir analysis_out
uv run python -m metrics.cli risk --out-dir analysis_out
uv run python -m metrics.cli structure --out-dir analysis_out
uv run python -m metrics.cli ccf --out-dir analysis_out
uv run python -m metrics.cli granger --out-dir analysis_out
uv run python -m metrics.cli dcc --out-dir analysis_out
```

静态前端的数据导出与构建：

```bash
cd coingeko
uv run python -m metrics.cli web-export \
  --analysis-dir analysis_out \
  --out-dir webapp/public/data \
  --top-pairs 24 \
  --heatmap-coins 16

cd webapp
npm install
npm run build
```

开发模式：

```bash
cd coingeko/webapp
npm run dev
```

核心输出目录：

- `analysis_out/prepared_prices.csv`
- `analysis_out/returns_wide.csv`
- `analysis_out/pairwise_correlation.csv`
- `analysis_out/rolling_correlation.csv`
- `analysis_out/cointegration.csv`
- `analysis_out/ccf.csv`
- `analysis_out/granger.csv`
- `analysis_out/market_exposure.csv`
- `analysis_out/risk_metrics.csv`
- `analysis_out/pca_summary.csv`
- `analysis_out/pca_loadings.csv`
- `analysis_out/clustering_linkage.csv`
- `analysis_out/centrality.csv`
- `analysis_out/dcc_garch.csv`
