import { ReactNode, useState } from "react";

type Column<T> = {
  key: string;
  label: string;
  render: (row: T) => ReactNode;
  sortValue?: (row: T) => number | string | null;
  className?: string;
};

type DataTableProps<T> = {
  rows: T[];
  columns: Array<Column<T>>;
  getRowKey?: (row: T, index: number) => string;
  emptyMessage?: string;
  initialSortKey?: string | null;
  initialDescending?: boolean;
};

export function DataTable<T>({
  rows,
  columns,
  getRowKey,
  emptyMessage = "No rows match the current filters.",
  initialSortKey = null,
  initialDescending = true,
}: DataTableProps<T>) {
  const [sortKey, setSortKey] = useState<string | null>(initialSortKey);
  const [descending, setDescending] = useState(initialDescending);

  const activeColumn = columns.find((column) => column.key === sortKey);
  const sortedRows = activeColumn?.sortValue
    ? [...rows].sort((left: T, right: T) => {
        const a = activeColumn.sortValue?.(left);
        const b = activeColumn.sortValue?.(right);
        if (a === null || a === undefined) return 1;
        if (b === null || b === undefined) return -1;
        if (a < b) return descending ? 1 : -1;
        if (a > b) return descending ? -1 : 1;
        return 0;
      })
    : rows;

  return (
    <div className="table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column.key}>
                {column.sortValue ? (
                  <button
                    className="table-sort"
                    onClick={() => {
                      if (sortKey === column.key) {
                        setDescending((current) => !current);
                      } else {
                        setSortKey(column.key);
                        setDescending(true);
                      }
                    }}
                    type="button"
                  >
                    {column.label}
                  </button>
                ) : (
                  column.label
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sortedRows.length === 0 ? (
            <tr>
              <td className="table-empty" colSpan={columns.length}>
                {emptyMessage}
              </td>
            </tr>
          ) : (
            sortedRows.map((row: T, index: number) => (
              <tr key={getRowKey ? getRowKey(row, index) : `${index}`}>
                {columns.map((column) => (
                  <td key={column.key} className={column.className}>
                    {column.render(row)}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
