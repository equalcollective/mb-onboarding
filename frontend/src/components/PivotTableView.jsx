import React, { useState, useEffect, useMemo, useCallback } from 'react'

const METRIC_PRESETS = {
  sales_overview: { label: 'Sales Overview', metrics: ['total_sales', 'sessions', 'units', 'cvr_pct'] },
  advertising: { label: 'Advertising', metrics: ['ad_spend', 'ad_sales', 'roas', 'acos_pct', 'impressions', 'clicks', 'ctr_pct'] },
  conversion: { label: 'Conversion', metrics: ['sessions', 'page_views', 'total_order_items', 'cvr_pct', 'unit_session_pct'] },
  traffic: { label: 'Traffic', metrics: ['sessions', 'page_views', 'page_views_per_session'] },
  all: { label: 'All Metrics', metrics: [] }
}

function formatValue(value, columnName) {
  if (value == null || value === '') return '-'

  if (columnName.includes('sales') || columnName.includes('spend') || columnName.includes('price')) {
    return new Intl.NumberFormat('en-US', {
      style: 'currency', currency: 'USD', minimumFractionDigits: 0, maximumFractionDigits: 0
    }).format(value)
  }
  if (columnName.includes('pct') || columnName.includes('cvr') || columnName.includes('ctr') || columnName.includes('acos')) {
    return `${Number(value).toFixed(1)}%`
  }
  if (columnName.includes('roas')) {
    return `${Number(value).toFixed(2)}x`
  }
  if (typeof value === 'number') {
    return new Intl.NumberFormat('en-US').format(Math.round(value))
  }
  return String(value)
}

function PivotTableView({ sellerName, asinHierarchy }) {
  // Filters
  const [aggregationLevel, setAggregationLevel] = useState('parent')
  const [granularity, setGranularity] = useState('weekly')
  const [metricPreset, setMetricPreset] = useState('sales_overview')
  const [selectedPeriods, setSelectedPeriods] = useState([])
  const [includeTotals, setIncludeTotals] = useState(true)

  // Available periods
  const [availablePeriods, setAvailablePeriods] = useState([])

  // Column ordering
  const [columnOrder, setColumnOrder] = useState([])
  const [draggedColumn, setDraggedColumn] = useState(null)

  // Data
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  // Fetch available periods
  useEffect(() => {
    if (sellerName) {
      fetchAvailablePeriods()
    }
  }, [sellerName, granularity])

  // Fetch pivot data
  useEffect(() => {
    if (sellerName) {
      fetchPivotData()
    }
  }, [sellerName, aggregationLevel, granularity, metricPreset, selectedPeriods, includeTotals])

  const fetchAvailablePeriods = async () => {
    try {
      const response = await fetch(`/api/seller/${sellerName}/metrics`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ aggregation_level: 'account', granularity })
      })
      if (response.ok) {
        const result = await response.json()
        const periods = [...new Set(result.data.map(d => d.period_start_date))].sort().reverse()
        setAvailablePeriods(periods)
      }
    } catch (err) {
      console.error('Failed to fetch periods:', err)
    }
  }

  const fetchPivotData = async () => {
    if (!sellerName) return

    setLoading(true)
    setError(null)

    try {
      const requestBody = {
        aggregation_level: aggregationLevel,
        granularity,
        metric_preset: metricPreset,
        include_totals: includeTotals
      }

      // Add date filters if periods selected
      if (selectedPeriods.length > 0) {
        const sorted = [...selectedPeriods].sort()
        requestBody.start_date = sorted[0]
        requestBody.end_date = sorted[sorted.length - 1]
      }

      const response = await fetch(`/api/seller/${sellerName}/pivot`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      })

      if (!response.ok) throw new Error('Failed to fetch pivot data')

      const result = await response.json()
      setData(result)

      // Initialize column order from data columns
      if (result.columns) {
        const metricCols = result.columns.filter(c =>
          !['seller_id', 'seller_name', 'adjusted_normalized_name', 'child_asin', 'parent_asin'].includes(c)
        )
        setColumnOrder(metricCols)
      }

    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const togglePeriod = (period) => {
    setSelectedPeriods(prev => {
      if (prev.includes(period)) {
        return prev.filter(p => p !== period)
      }
      return [...prev, period].sort().reverse()
    })
  }

  // Drag and drop handlers
  const handleDragStart = (e, column) => {
    setDraggedColumn(column)
    e.dataTransfer.effectAllowed = 'move'
  }

  const handleDragOver = (e) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
  }

  const handleDrop = (e, targetColumn) => {
    e.preventDefault()
    if (!draggedColumn || draggedColumn === targetColumn) return

    const newOrder = [...columnOrder]
    const draggedIdx = newOrder.indexOf(draggedColumn)
    const targetIdx = newOrder.indexOf(targetColumn)

    newOrder.splice(draggedIdx, 1)
    newOrder.splice(targetIdx, 0, draggedColumn)

    setColumnOrder(newOrder)
    setDraggedColumn(null)
  }

  // Build parent ASIN lookup from hierarchy
  const childToParentMap = useMemo(() => {
    const map = {}
    asinHierarchy.forEach(parent => {
      parent.children.forEach(child => {
        map[child.child_asin] = parent.parent_name
      })
    })
    return map
  }, [asinHierarchy])

  // Process display data
  const displayData = useMemo(() => {
    if (!data?.data) return []

    return data.data.map(row => {
      const newRow = { ...row }

      // Add parent name for child-level view
      if (aggregationLevel === 'child' && row.child_asin) {
        newRow._parentName = childToParentMap[row.child_asin] || '-'
      }

      return newRow
    })
  }, [data, aggregationLevel, childToParentMap])

  // Get the identifier column(s) based on aggregation level
  const idColumns = useMemo(() => {
    if (aggregationLevel === 'child') {
      return ['child_asin', '_parentName']
    }
    return ['adjusted_normalized_name']
  }, [aggregationLevel])

  const idLabels = {
    'child_asin': 'ASIN',
    '_parentName': 'Parent Product',
    'adjusted_normalized_name': 'Product'
  }

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="bg-white rounded-lg shadow-sm border p-4">
        <div className="flex flex-wrap items-start gap-6">
          {/* Aggregation Level */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">View By</label>
            <select
              value={aggregationLevel}
              onChange={(e) => setAggregationLevel(e.target.value)}
              className="px-3 py-1.5 border border-gray-300 rounded bg-white text-sm"
            >
              <option value="account">Account Level</option>
              <option value="parent">By Product (Parent)</option>
              <option value="child">By ASIN (Child)</option>
            </select>
          </div>

          {/* Granularity */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Period</label>
            <select
              value={granularity}
              onChange={(e) => {
                setGranularity(e.target.value)
                setSelectedPeriods([])
              }}
              className="px-3 py-1.5 border border-gray-300 rounded bg-white text-sm"
            >
              <option value="weekly">Weekly</option>
              <option value="monthly">Monthly</option>
            </select>
          </div>

          {/* Metric Preset */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Metrics</label>
            <select
              value={metricPreset}
              onChange={(e) => setMetricPreset(e.target.value)}
              className="px-3 py-1.5 border border-gray-300 rounded bg-white text-sm"
            >
              {Object.entries(METRIC_PRESETS).map(([key, preset]) => (
                <option key={key} value={key}>{preset.label}</option>
              ))}
            </select>
          </div>

          {/* Include Totals */}
          <div className="flex items-end pb-1">
            <label className="flex items-center gap-2 text-sm text-gray-600 cursor-pointer">
              <input
                type="checkbox"
                checked={includeTotals}
                onChange={(e) => setIncludeTotals(e.target.checked)}
                className="rounded border-gray-300"
              />
              Show Totals
            </label>
          </div>
        </div>

        {/* Time Period Multi-Select */}
        <div className="mt-4">
          <div className="flex items-center justify-between mb-2">
            <label className="text-sm font-medium text-gray-700">
              Compare Periods ({selectedPeriods.length} selected)
            </label>
            <div className="flex gap-2">
              <button
                onClick={() => setSelectedPeriods(availablePeriods.slice(0, 4))}
                className="text-xs text-blue-600 hover:text-blue-800"
              >
                Last 4
              </button>
              <button
                onClick={() => setSelectedPeriods(availablePeriods.slice(0, 8))}
                className="text-xs text-blue-600 hover:text-blue-800"
              >
                Last 8
              </button>
              <button
                onClick={() => setSelectedPeriods([])}
                className="text-xs text-gray-600 hover:text-gray-800"
              >
                Clear (All)
              </button>
            </div>
          </div>
          <div className="flex flex-wrap gap-2 max-h-24 overflow-y-auto p-2 bg-gray-50 rounded border">
            {availablePeriods.map((period) => (
              <button
                key={period}
                onClick={() => togglePeriod(period)}
                className={`px-2 py-1 text-xs rounded border transition-colors ${
                  selectedPeriods.includes(period)
                    ? 'bg-blue-100 border-blue-300 text-blue-700'
                    : 'bg-white border-gray-300 text-gray-600 hover:bg-gray-100'
                }`}
              >
                {formatPeriodLabel(period, granularity)}
              </button>
            ))}
          </div>
          {selectedPeriods.length === 0 && (
            <p className="text-xs text-gray-500 mt-1">No periods selected - showing all available data</p>
          )}
        </div>
      </div>

      {/* Column Reorder Instructions */}
      {columnOrder.length > 0 && (
        <div className="text-xs text-gray-500 bg-blue-50 border border-blue-200 rounded px-3 py-2">
          Tip: Drag column headers to reorder them
        </div>
      )}

      {/* Data Table */}
      {loading ? (
        <div className="bg-white rounded-lg shadow-sm border p-8 text-center text-gray-500">
          Loading...
        </div>
      ) : error ? (
        <div className="bg-white rounded-lg shadow-sm border p-8 text-center text-red-500">
          {error}
        </div>
      ) : displayData.length === 0 ? (
        <div className="bg-white rounded-lg shadow-sm border p-8 text-center text-gray-500">
          No data available
        </div>
      ) : (
        <div className="bg-white rounded-lg shadow-sm border overflow-hidden">
          <div className="px-4 py-3 border-b bg-gray-50">
            <div className="flex items-center justify-between">
              <h2 className="font-medium text-gray-900">
                Pivot Table - {METRIC_PRESETS[metricPreset].label}
              </h2>
              <span className="text-sm text-gray-500">
                {data.count} rows | {data.periods?.length || 0} periods | {data.metrics?.length || 0} metrics
              </span>
            </div>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b">
                  {/* ID Columns (fixed) */}
                  {idColumns.map((col) => (
                    <th
                      key={col}
                      className="text-left px-4 py-3 font-medium text-gray-700 sticky left-0 bg-gray-50 z-10"
                    >
                      {idLabels[col] || col}
                    </th>
                  ))}
                  {/* Metric Columns (draggable) */}
                  {columnOrder.slice(0, 30).map((col) => (
                    <th
                      key={col}
                      draggable
                      onDragStart={(e) => handleDragStart(e, col)}
                      onDragOver={handleDragOver}
                      onDrop={(e) => handleDrop(e, col)}
                      className={`text-right px-3 py-3 font-medium text-gray-700 whitespace-nowrap cursor-move select-none ${
                        draggedColumn === col ? 'bg-blue-100' : ''
                      }`}
                    >
                      {formatColumnHeader(col)}
                    </th>
                  ))}
                  {columnOrder.length > 30 && (
                    <th className="text-center px-3 py-3 text-gray-400">
                      +{columnOrder.length - 30} more
                    </th>
                  )}
                </tr>
              </thead>
              <tbody>
                {displayData.map((row, idx) => {
                  const isTotal = row.adjusted_normalized_name === 'TOTAL' || row.child_asin === 'TOTAL'
                  return (
                    <tr
                      key={idx}
                      className={`border-b hover:bg-gray-50 ${isTotal ? 'bg-blue-50 font-medium' : ''}`}
                    >
                      {/* ID Columns */}
                      {idColumns.map((col, colIdx) => (
                        <td
                          key={col}
                          className={`px-4 py-3 sticky z-10 ${isTotal ? 'bg-blue-50' : 'bg-white'}`}
                          style={{ left: colIdx * 150 }}
                        >
                          {row[col] || '-'}
                        </td>
                      ))}
                      {/* Metric Columns */}
                      {columnOrder.slice(0, 30).map((col) => (
                        <td key={col} className="text-right px-3 py-3 whitespace-nowrap">
                          {formatValue(row[col], col)}
                        </td>
                      ))}
                      {columnOrder.length > 30 && (
                        <td className="text-center px-3 py-3 text-gray-400">...</td>
                      )}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          {columnOrder.length > 30 && (
            <div className="px-4 py-3 border-t bg-gray-50 text-sm text-gray-500">
              Showing first 30 columns. Export to CSV for full data.
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function formatPeriodLabel(dateStr, granularity) {
  if (!dateStr) return '-'
  const date = new Date(dateStr + 'T00:00:00')
  if (granularity === 'monthly') {
    return date.toLocaleDateString('en-US', { month: 'short', year: 'numeric' })
  }
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

function formatColumnHeader(col) {
  const parts = col.split('_')
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

  return col.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())
}

export default PivotTableView
