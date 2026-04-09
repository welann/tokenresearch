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
};

export function DataTable<T>({ rows, columns }: DataTableProps<T>) {
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [descending, setDescending] = useState(true);

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
          {sortedRows.map((row: T, index: number) => (
            <tr key={index}>
              {columns.map((column) => (
                <td key={column.key} className={column.className}>
                  {column.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
