import React, { useState } from 'react';
import PieChart from './PieChart';

const Renderer = () => {
  const [data, setData] = useState([10, 20, 30, 40]);
  const [highlightIndex, setHighlightIndex] = useState(null);

  const handleMouseEnter = (index) => {
    setHighlightIndex(index);
  };

  const handleMouseLeave = () => {
    setHighlightIndex(null);
  };

  return (
    <div>
      <PieChart data={data} highlightIndex={highlightIndex} />
      <div>
        {data.map((value, i) => (
          <div
            key={i}
            onMouseEnter={() => handleMouseEnter(i)}
            onMouseLeave={handleMouseLeave}
          >
            {value}
          </div>
        ))}
      </div>
    </div>
  );
};

export default Renderer;
