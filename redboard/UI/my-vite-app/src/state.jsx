import React, { useState } from 'react';
import './App.css';

function Switch_context() {
  const [color, setColor] = useState('red');
  const [heading, setHeading] = useState('machines');

  const toggleColorAndHeading = () => {
    if (color === 'red') {
      setColor('yellow');
      setHeading('executers');
    } else {
      setColor('red');
      setHeading('machines');
    }
  };

  return (
    <div className="App" style={{ backgroundColor: color }}>
      <h1>{heading}</h1>
      <button
        onClick={toggleColorAndHeading}
        style={{
          borderRadius: '50%',
          padding: '10px 20px',
          border: 'none',
          cursor: 'pointer',
        }}
      >
        Toggle
      </button>
    </div>
  );
}

export default Switch_context;