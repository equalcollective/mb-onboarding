import React from 'react'

function formatCurrency(value) {
  if (value == null) return '-'
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0
  }).format(value)
}

function formatNumber(value) {
  if (value == null) return '-'
  return new Intl.NumberFormat('en-US').format(Math.round(value))
}

function formatPercent(value) {
  if (value == null) return '-'
  return `${value.toFixed(1)}%`
}

function SummaryCards({ data }) {
  if (!data) return null

  const cards = [
    {
      label: 'Total Sales',
      value: formatCurrency(data.total_sales),
      color: 'bg-blue-50 text-blue-700'
    },
    {
      label: 'Total Units',
      value: formatNumber(data.total_units),
      color: 'bg-green-50 text-green-700'
    },
    {
      label: 'Sessions',
      value: formatNumber(data.sessions),
      color: 'bg-purple-50 text-purple-700'
    },
    {
      label: 'Conversion Rate',
      value: formatPercent(data.cvr_pct),
      color: 'bg-orange-50 text-orange-700'
    },
    {
      label: 'Ad Spend',
      value: formatCurrency(data.ad_spend),
      color: 'bg-red-50 text-red-700'
    },
    {
      label: 'ROAS',
      value: data.roas != null ? `${data.roas.toFixed(2)}x` : '-',
      color: 'bg-teal-50 text-teal-700'
    }
  ]

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4 mb-6">
      {cards.map((card) => (
        <div
          key={card.label}
          className={`rounded-lg p-4 ${card.color}`}
        >
          <div className="text-sm font-medium opacity-80">{card.label}</div>
          <div className="text-xl font-bold mt-1">{card.value}</div>
        </div>
      ))}
    </div>
  )
}

export default SummaryCards
