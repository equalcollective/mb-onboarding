import React from 'react'

const AGGREGATION_OPTIONS = [
  { value: 'account', label: 'Account Level' },
  { value: 'parent', label: 'By Product' },
  { value: 'child', label: 'By ASIN' }
]

const GRANULARITY_OPTIONS = [
  { value: 'weekly', label: 'Weekly' },
  { value: 'monthly', label: 'Monthly' }
]

const PRESET_OPTIONS = [
  { value: 'sales_overview', label: 'Sales Overview' },
  { value: 'advertising', label: 'Advertising' },
  { value: 'conversion', label: 'Conversion' },
  { value: 'traffic', label: 'Traffic' },
  { value: 'all', label: 'All Metrics' }
]

function Filters({ filters, onChange }) {
  const updateFilter = (key, value) => {
    onChange({ ...filters, [key]: value })
  }

  return (
    <div className="bg-white rounded-lg shadow-sm border p-4 mb-6">
      <div className="flex flex-wrap items-center gap-4">
        <div className="flex items-center gap-2">
          <label className="text-sm text-gray-600">View:</label>
          <select
            value={filters.aggregationLevel}
            onChange={(e) => updateFilter('aggregationLevel', e.target.value)}
            className="px-3 py-1.5 border border-gray-300 rounded bg-white text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {AGGREGATION_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>
        </div>

        <div className="flex items-center gap-2">
          <label className="text-sm text-gray-600">Period:</label>
          <select
            value={filters.granularity}
            onChange={(e) => updateFilter('granularity', e.target.value)}
            className="px-3 py-1.5 border border-gray-300 rounded bg-white text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {GRANULARITY_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>
        </div>

        <div className="flex items-center gap-2">
          <label className="text-sm text-gray-600">Metrics:</label>
          <select
            value={filters.metricPreset}
            onChange={(e) => updateFilter('metricPreset', e.target.value)}
            className="px-3 py-1.5 border border-gray-300 rounded bg-white text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {PRESET_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>
        </div>

        <div className="flex items-center gap-2">
          <label className="flex items-center gap-2 text-sm text-gray-600 cursor-pointer">
            <input
              type="checkbox"
              checked={filters.includeTotals}
              onChange={(e) => updateFilter('includeTotals', e.target.checked)}
              className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
            />
            Show Totals
          </label>
        </div>
      </div>
    </div>
  )
}

export default Filters
