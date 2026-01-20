import React, { useMemo } from 'react'

function formatValue(value, columnName) {
  if (value == null || value === '') return '-'

  // Currency columns
  if (columnName.includes('sales') || columnName.includes('spend') || columnName.includes('price')) {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(value)
  }

  // Percentage columns
  if (columnName.includes('pct') || columnName.includes('cvr') || columnName.includes('ctr') || columnName.includes('acos')) {
    return `${Number(value).toFixed(1)}%`
  }

  // ROAS
  if (columnName.includes('roas')) {
    return `${Number(value).toFixed(2)}x`
  }

  // Numbers
  if (typeof value === 'number') {
    return new Intl.NumberFormat('en-US').format(Math.round(value))
  }

  return String(value)
}

function MetricsTable({ data, filters }) {
  const { columns, displayData, periods } = useMemo(() => {
    if (!data || !data.data || data.data.length === 0) {
      return { columns: [], displayData: [], periods: [] }
    }

    // Get all columns
    const allColumns = data.columns || []

    // Identify row identifier columns
    const idColumns = allColumns.filter(col =>
      col === 'seller_id' ||
      col === 'adjusted_normalized_name' ||
      col === 'child_asin' ||
      col === 'parent_asin'
    )

    // Get metric columns (date-prefixed)
    const metricColumns = allColumns.filter(col => !idColumns.includes(col))

    return {
      columns: { idColumns, metricColumns },
      displayData: data.data,
      periods: data.periods || []
    }
  }, [data])

  if (!data || !data.data || data.data.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow-sm border p-8 text-center text-gray-500">
        No data available. Select a seller to view metrics.
      </div>
    )
  }

  // Determine which name column to show based on aggregation level
  const nameColumn = filters.aggregationLevel === 'child'
    ? 'child_asin'
    : 'adjusted_normalized_name'

  const nameLabel = filters.aggregationLevel === 'child'
    ? 'ASIN'
    : filters.aggregationLevel === 'parent'
      ? 'Product'
      : 'Account'

  return (
    <div className="bg-white rounded-lg shadow-sm border overflow-hidden">
      <div className="px-4 py-3 border-b bg-gray-50">
        <div className="flex items-center justify-between">
          <h2 className="font-medium text-gray-900">
            Metrics by {nameLabel}
          </h2>
          <span className="text-sm text-gray-500">
            {data.count} rows | {periods.length} periods | {data.metrics?.length || 0} metrics
          </span>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b">
              <th className="text-left px-4 py-3 font-medium text-gray-700 sticky left-0 bg-gray-50 z-10">
                {nameLabel}
              </th>
              {columns.metricColumns.slice(0, 20).map((col) => (
                <th
                  key={col}
                  className="text-right px-3 py-3 font-medium text-gray-700 whitespace-nowrap"
                >
                  {formatColumnHeader(col)}
                </th>
              ))}
              {columns.metricColumns.length > 20 && (
                <th className="text-center px-3 py-3 font-medium text-gray-400">
                  +{columns.metricColumns.length - 20} more
                </th>
              )}
            </tr>
          </thead>
          <tbody>
            {displayData.map((row, idx) => {
              const isTotal = row[nameColumn] === 'TOTAL' || row.adjusted_normalized_name === 'TOTAL'
              return (
                <tr
                  key={idx}
                  className={`border-b hover:bg-gray-50 ${isTotal ? 'bg-blue-50 font-medium' : ''}`}
                >
                  <td className={`px-4 py-3 sticky left-0 z-10 ${isTotal ? 'bg-blue-50' : 'bg-white'}`}>
                    {row[nameColumn] || row.adjusted_normalized_name || '-'}
                  </td>
                  {columns.metricColumns.slice(0, 20).map((col) => (
                    <td key={col} className="text-right px-3 py-3 whitespace-nowrap">
                      {formatValue(row[col], col)}
                    </td>
                  ))}
                  {columns.metricColumns.length > 20 && (
                    <td className="text-center px-3 py-3 text-gray-400">...</td>
                  )}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {columns.metricColumns.length > 20 && (
        <div className="px-4 py-3 border-t bg-gray-50 text-sm text-gray-500">
          Showing first 20 columns. Export to CSV for full data.
        </div>
      )}
    </div>
  )
}

function formatColumnHeader(col) {
  // Convert "Jan_11_total_sales" to "Jan 11 Sales"
  const parts = col.split('_')

  // Check if it starts with a month abbreviation
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
  if (months.includes(parts[0])) {
    const date = `${parts[0]} ${parts[1]}`
    const metric = parts.slice(2).join(' ')
      .replace('total ', '')
      .replace('pct', '%')
      .replace('cvr', 'CVR')
      .replace('ctr', 'CTR')
      .replace('roas', 'ROAS')
      .replace('acos', 'ACOS')
    return (
      <div className="flex flex-col">
        <span className="text-xs text-gray-400">{date}</span>
        <span>{metric}</span>
      </div>
    )
  }

  // Regular column name
  return col.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())
}

export default MetricsTable
