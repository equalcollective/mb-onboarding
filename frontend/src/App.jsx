import React, { useState, useEffect } from 'react'
import Header from './components/Header'
import TimeSeriesView from './components/TimeSeriesView'
import PivotTableView from './components/PivotTableView'

function App() {
  const [sellers, setSellers] = useState([])
  const [selectedSeller, setSelectedSeller] = useState(null)
  const [activeView, setActiveView] = useState('timeseries') // 'timeseries' or 'pivot'
  const [error, setError] = useState(null)

  // Seller's ASIN hierarchy for filtering
  const [asinHierarchy, setAsinHierarchy] = useState([])

  // Fetch sellers on mount
  useEffect(() => {
    fetchSellers()
  }, [])

  // Fetch ASIN hierarchy when seller changes
  useEffect(() => {
    if (selectedSeller) {
      fetchAsinHierarchy()
    }
  }, [selectedSeller])

  const fetchSellers = async () => {
    try {
      const response = await fetch('/api/sellers')
      const data = await response.json()
      setSellers(data)
      if (data.length > 0) {
        setSelectedSeller(data[0].seller_name)
      }
    } catch (err) {
      setError('Failed to load sellers. Is the backend running?')
    }
  }

  const fetchAsinHierarchy = async () => {
    try {
      const response = await fetch(`/api/seller/${selectedSeller}/asins`)
      const data = await response.json()
      setAsinHierarchy(data)
    } catch (err) {
      console.error('Failed to fetch ASIN hierarchy:', err)
    }
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <Header
        sellers={sellers}
        selectedSeller={selectedSeller}
        onSellerChange={setSelectedSeller}
      />

      {/* View Tabs */}
      <div className="bg-white border-b">
        <div className="max-w-7xl mx-auto px-4">
          <nav className="flex gap-4">
            <button
              onClick={() => setActiveView('timeseries')}
              className={`py-3 px-4 border-b-2 font-medium text-sm transition-colors ${
                activeView === 'timeseries'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              Time Series
            </button>
            <button
              onClick={() => setActiveView('pivot')}
              className={`py-3 px-4 border-b-2 font-medium text-sm transition-colors ${
                activeView === 'pivot'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              Pivot Table
            </button>
          </nav>
        </div>
      </div>

      <main className="max-w-7xl mx-auto px-4 py-6">
        {error && (
          <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-lg text-red-700">
            {error}
          </div>
        )}

        {activeView === 'timeseries' ? (
          <TimeSeriesView
            sellerName={selectedSeller}
            asinHierarchy={asinHierarchy}
          />
        ) : (
          <PivotTableView
            sellerName={selectedSeller}
            asinHierarchy={asinHierarchy}
          />
        )}
      </main>
    </div>
  )
}

export default App
