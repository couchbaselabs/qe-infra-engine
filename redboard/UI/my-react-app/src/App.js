import React, { useState } from 'react';
import { PieChart } from '@mui/x-charts/PieChart';

const initialData = [
  { id: 0, value: 10, label: 'Apple' },
  { id: 1, value: 15, label: 'Banana' },
  { id: 2, value: 20, label: 'Cherry' },
];

const App = () => {
  const [data, setData] = useState(initialData);

  const handleFilterChange = (selectedFilters) => {
    // Filter data based on selected filters
    const filteredData = initialData.filter(item => selectedFilters.includes(item.label));
    setData(filteredData);
  };

  return (
    <div>
      <Sidebar onFilterChange={handleFilterChange} />
      <PieChart series={[{ data }]} width={400} height={200} />
    </div>
  );
};

const Sidebar = ({ onFilterChange }) => {
  const [selectedFilters, setSelectedFilters] = useState(['Apple', 'Banana', 'Cherry']);

  const handleCheckboxChange = (event) => {
    const { value, checked } = event.target;
    const newFilters = checked
      ? [...selectedFilters, value]
      : selectedFilters.filter(filter => filter !== value);

    setSelectedFilters(newFilters);
    onFilterChange(newFilters);
  };

  return (
    <div className="sidebar">
      <label>
        <input
          type="checkbox"
          value="Apple"
          checked={selectedFilters.includes('Apple')}
          onChange={handleCheckboxChange}
        />
        Apple
      </label>
      {/* Repeat for other filters */}
    </div>
  );
};


export default App;
