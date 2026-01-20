import React, { useState, useEffect, useMemo } from 'react'

const METRIC_GROUPS = {
  ads: {
    label: 'Advertising Only',
    metrics: ['ad_spend', 'ad_sales', 'roas', 'acos_pct', 'impressions', 'clicks', 'ctr_pct', 'cpc']
  },
  business: {
    label: 'Business Only',
    metrics: ['total_sales', 'total_units', 'sessions', 'page_views', 'cvr_pct', 'unit_session_pct', 'avg_price']
  },
  combined: {
    label: 'All Metrics',
    metrics: ['total_sales', 'total_units', 'sessions', 'cvr_pct', 'ad_spend', 'ad_sales', 'roas', 'acos_pct', 'organic_sales', 'tacos_pct']
  }
}

function formatValue(value, metric) {
  if (value == null || value === '') return '-'

  if (metric.includes('sales') || metric.includes('spend') || metric.includes('price') || metric === 'cpc') {
    return new Intl.NumberFormat('en-US', {
      style: 'currency', currency: 'USD', minimumFractionDigits: 0, maximumFractionDigits: 0
    }).format(value)
  }
  if (metric.includes('pct') || metric.includes('cvr') || metric.includes('ctr') || metric.includes('acos') || metric.includes('tacos')) {
    return `${Number(value).toFixed(1)}%`
  }
  if (metric.includes('roas')) {
    return `${Number(value).toFixed(2)}x`
  }
  return new Intl.NumberFormat('en-US').format(Math.round(value))
}

function formatMetricName(metric) {
  return metric
    .replace(/_/g, ' ')
    .replace('pct', '%')
    .replace('cvr', 'CVR')
    .replace('ctr', 'CTR')
    .replace('roas', 'ROAS')
    .replace('acos', 'ACOS')
    .replace('tacos', 'TACoS')
    .replace('cpc', 'CPC')
    .replace(/\b\w/g, l => l.toUpperCase())
}

function TimeSeriesView({ sellerName, asinHierarchy }) {
  // Filters
  const [granularity, setGranularity] = useState('weekly')
  const [metricGroup, setMetricGroup] = useState('combined')
  const [selectedParent, setSelectedParent] = useState('all')
  const [selectedPeriods, setSelectedPeriods] = useState([])

  // Available periods from the data
  const [availablePeriods, setAvailablePeriods] = useState([])

  // Data
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  // Fetch available periods when seller or granularity changes
  useEffect(() => {
    if (sellerName) {
      fetchAvailablePeriods()
    }
  }, [sellerName, granularity])

  // Fetch data when filters change
  useEffect(() => {
    if (sellerName && selectedPeriods.length > 0) {
      fetchData()
    } else if (sellerName && selectedPeriods.length === 0) {
      // Fetch all data if no specific periods selected
      fetchData()
    }
  }, [sellerName, granularity, metricGroup, selectedParent, selectedPeriods])

  const fetchAvailablePeriods = async () => {
    try {
      // Fetch metrics to get available periods
      const response = await fetch(`/api/seller/${sellerName}/metrics`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          aggregation_level: 'account',
          granularity: granularity
        })
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

  const fetchData = async () => {
    if (!sellerName) return

    setLoading(true)
    setError(null)

    try {
      const requestBody = {
        aggregation_level: selectedParent === 'all' ? 'account' : 'parent',
        granularity: granularity,
        include_comparison: false
      }

      // Add parent filter if selected
      if (selectedParent !== 'all') {
        requestBody.parent_asins = [selectedParent]
      }

      // Add specific periods if selected
      if (selectedPeriods.length > 0) {
        if (granularity === 'weekly') {
          requestBody.specific_weeks = selectedPeriods
        } else {
          requestBody.specific_months = selectedPeriods
        }
      }

      const response = await fetch(`/api/seller/${sellerName}/metrics`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      })

      if (!response.ok) {
        throw new Error('Failed to fetch data')
      }

      const result = await response.json()
      setData(result)

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
      } else {
        return [...prev, period].sort().reverse()
      }
    })
  }

  const selectAllPeriods = () => {
    setSelectedPeriods(availablePeriods)
  }

  const clearPeriods = () => {
    setSelectedPeriods([])
  }

  // Get the metrics to display based on selected group
  const metricsToShow = METRIC_GROUPS[metricGroup].metrics

  // Filter and format data for display
  const displayData = useMemo(() => {
    if (!data?.data) return []

    return data.data.map(row => {
      const formatted = { period: row.period_start_date }
      metricsToShow.forEach(metric => {
        if (row[metric] !== undefined) {
          formatted[metric] = row[metric]
        }
      })
      return formatted
    }).sort((a, b) => b.period.localeCompare(a.period))
  }, [data, metricsToShow])

  // Calculate totals
  const totals = useMemo(() => {
    if (displayData.length === 0) return null

    const sums = {}
    const counts = {}

    metricsToShow.forEach(metric => {
      sums[metric] = 0
      counts[metric] = 0
    })

    displayData.forEach(row => {
      metricsToShow.forEach(metric => {
        if (row[metric] != null && !isNaN(row[metric])) {
          sums[metric] += row[metric]
          counts[metric]++
        }
      })
    })

    // For percentages and ratios, use average; for counts use sum
    const result = {}
    metricsToShow.forEach(metric => {
      if (metric.includes('pct') || metric === 'roas' || metric === 'cpc' || metric === 'avg_price') {
        result[metric] = counts[metric] > 0 ? sums[metric] / counts[metric] : null
      } else {
        result[metric] = sums[metric]
      }
    })

    return result
  }, [displayData, metricsToShow])

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="bg-white rounded-lg shadow-sm border p-4">
        <div className="flex flex-wrap items-start gap-6">
          {/* Granularity */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Granularity</label>
            <select
              value={granularity}
              onChange={(e) => {
                setGranularity(e.target.value)
                setSelectedPeriods([])
              }}
              className="px-3 py-1.5 border border-gray-300 rounded bg-white text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="weekly">Weekly</option>
              <option value="monthly">Monthly</option>
            </select>
          </div>

          {/* Metric Group */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Metrics</label>
            <select
              value={metricGroup}
              onChange={(e) => setMetricGroup(e.target.value)}
              className="px-3 py-1.5 border border-gray-300 rounded bg-white text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {Object.entries(METRIC_GROUPS).map(([key, group]) => (
                <option key={key} value={key}>{group.label}</option>
              ))}
            </select>
          </div>

          {/* Parent ASIN Filter */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Product (Parent)</label>
            <select
              value={selectedParent}
              onChange={(e) => setSelectedParent(e.target.value)}
              className="px-3 py-1.5 border border-gray-300 rounded bg-white text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 min-w-[200px]"
            >
              <option value="all">All Products</option>
              {asinHierarchy.map((parent) => (
                <option key={parent.parent_name} value={parent.parent_name}>
                  {parent.parent_name} ({parent.child_count})
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Time Period Multi-Select */}
        <div className="mt-4">
          <div className="flex items-center justify-between mb-2">
            <label className="text-sm font-medium text-gray-700">
              Time Periods ({selectedPeriods.length} selected)
            </label>
            <div className="flex gap-2">
              <button
                onClick={selectAllPeriods}
                className="text-xs text-blue-600 hover:text-blue-800"
              >
                Select All
              </button>
              <button
                onClick={clearPeriods}
                className="text-xs text-gray-600 hover:text-gray-800"
              >
                Clear
              </button>
            </div>
          </div>
          <div className="flex flex-wrap gap-2 max-h-32 overflow-y-auto p-2 bg-gray-50 rounded border">
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
                Time Series - {METRIC_GROUPS[metricGroup].label}
              </h2>
              <span className="text-sm text-gray-500">
                {displayData.length} periods | {metricsToShow.length} metrics
              </span>
            </div>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b">
                  <th className="text-left px-4 py-3 font-medium text-gray-700 sticky left-0 bg-gray-50">
                    Period
                  </th>
                  {metricsToShow.map((metric) => (
                    <th key={metric} className="text-right px-4 py-3 font-medium text-gray-700 whitespace-nowrap">
                      {formatMetricName(metric)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {displayData.map((row, idx) => (
                  <tr key={idx} className="border-b hover:bg-gray-50">
                    <td className="px-4 py-3 sticky left-0 bg-white font-medium">
                      {formatPeriodLabel(row.period, granularity)}
                    </td>
                    {metricsToShow.map((metric) => (
                      <td key={metric} className="text-right px-4 py-3 whitespace-nowrap">
                        {formatValue(row[metric], metric)}
                      </td>
                    ))}
                  </tr>
                ))}
                {/* Totals Row */}
                {totals && (
                  <tr className="bg-blue-50 font-medium border-t-2">
                    <td className="px-4 py-3 sticky left-0 bg-blue-50">
                      TOTAL / AVG
                    </td>
                    {metricsToShow.map((metric) => (
                      <td key={metric} className="text-right px-4 py-3 whitespace-nowrap">
                        {formatValue(totals[metric], metric)}
                      </td>
                    ))}
                  </tr>
                )}
              </tbody>
            </table>
          </div>
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

  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

export default TimeSeriesView
