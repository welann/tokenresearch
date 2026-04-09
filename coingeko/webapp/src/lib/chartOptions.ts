import type { EChartsOption } from "echarts";

const axisLabelStyle = {
  color: "#6a7280",
  fontFamily: "IBM Plex Mono",
  fontSize: 11,
};

const splitLine = {
  lineStyle: {
    color: "rgba(16, 32, 51, 0.08)",
  },
};

export function buildLineOption({
  legend = false,
  series,
}: {
  legend?: boolean;
  series: Array<{
    name: string;
    data: Array<[string, number | null]>;
    color?: string;
    area?: boolean;
  }>;
}): EChartsOption {
  return {
    backgroundColor: "transparent",
    tooltip: {
      trigger: "axis",
      backgroundColor: "#102033",
      borderWidth: 0,
      textStyle: {
        color: "#f7f2e8",
      },
    },
    legend: legend
      ? {
          top: 0,
          textStyle: {
            color: "#6a7280",
          },
        }
      : undefined,
    grid: {
      left: 56,
      right: 18,
      top: legend ? 42 : 18,
      bottom: 36,
    },
    xAxis: {
      type: "category",
      boundaryGap: false,
      axisLabel: axisLabelStyle,
      axisLine: { lineStyle: { color: "rgba(16, 32, 51, 0.1)" } },
    },
    yAxis: {
      type: "value",
      axisLabel: axisLabelStyle,
      splitLine,
    },
    series: series.map((item) => ({
      type: "line",
      name: item.name,
      data: item.data,
      showSymbol: false,
      smooth: true,
      lineStyle: {
        width: 2,
        color: item.color,
      },
      itemStyle: {
        color: item.color,
      },
      areaStyle: item.area
        ? {
            color: item.color ? `${item.color}20` : "rgba(153, 238, 76, 0.15)",
          }
        : undefined,
    })),
  };
}

export function buildBarOption({
  categories,
  values,
  color = "#99ee4c",
}: {
  categories: string[];
  values: Array<number | null>;
  color?: string;
}): EChartsOption {
  return {
    backgroundColor: "transparent",
    tooltip: {
      trigger: "axis",
      backgroundColor: "#102033",
      borderWidth: 0,
      textStyle: {
        color: "#f7f2e8",
      },
    },
    grid: {
      left: 56,
      right: 18,
      top: 18,
      bottom: 36,
    },
    xAxis: {
      type: "category",
      data: categories,
      axisLabel: axisLabelStyle,
      axisLine: { lineStyle: { color: "rgba(16, 32, 51, 0.1)" } },
    },
    yAxis: {
      type: "value",
      axisLabel: axisLabelStyle,
      splitLine,
    },
    series: [
      {
        type: "bar",
        data: values,
        itemStyle: {
          color,
          borderRadius: [8, 8, 0, 0],
        },
      },
    ],
  };
}

export function buildHeatmapOption({
  axes,
  rows,
}: {
  axes: string[];
  rows: Array<{ x: string; y: string; value: number | null }>;
}): EChartsOption {
  return {
    tooltip: {
      position: "top",
      backgroundColor: "#102033",
      borderWidth: 0,
      textStyle: {
        color: "#f7f2e8",
      },
    },
    grid: {
      left: 90,
      right: 18,
      top: 18,
      bottom: 60,
    },
    xAxis: {
      type: "category",
      data: axes,
      splitArea: { show: false },
      axisLabel: {
        ...axisLabelStyle,
        rotate: 35,
      },
    },
    yAxis: {
      type: "category",
      data: axes,
      splitArea: { show: false },
      axisLabel: axisLabelStyle,
    },
    visualMap: {
      min: -1,
      max: 1,
      calculable: false,
      orient: "horizontal",
      left: "center",
      bottom: 0,
      textStyle: {
        color: "#6a7280",
      },
      inRange: {
        color: ["#be5c3a", "#f1e6d4", "#0f766e"],
      },
    },
    series: [
      {
        type: "heatmap",
        data: rows.map((row) => [axes.indexOf(row.x), axes.indexOf(row.y), row.value]),
        label: { show: false },
      },
    ],
  };
}
