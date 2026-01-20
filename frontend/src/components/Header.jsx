import React from 'react'

function Header({ sellers, selectedSeller, onSellerChange }) {
  return (
    <header className="bg-white shadow-sm border-b">
      <div className="max-w-7xl mx-auto px-4 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <h1 className="text-xl font-semibold text-gray-900">
              Amazon Seller Analytics
            </h1>
          </div>

          <div className="flex items-center gap-3">
            <label className="text-sm text-gray-600">Seller:</label>
            <select
              value={selectedSeller || ''}
              onChange={(e) => onSellerChange(e.target.value)}
              className="px-3 py-2 border border-gray-300 rounded-lg bg-white text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              {sellers.map((seller) => (
                <option key={seller.seller_id} value={seller.seller_name}>
                  {seller.seller_name} ({seller.asin_count} ASINs)
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>
    </header>
  )
}

export default Header
