import React, { useState } from 'react';
import MachinePieChart from './PieChart';

const App = () => {
  const [remoteMachines, setDisplayState] = useState(false);
  const data = [
    { name: 'Group A', value: 400 },
    { name: 'Group B', value: 300 },
    { name: 'Group C', value: 300 },
    { name: 'Group D', value: 200 },
  ];


  return (
    <div style={{
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      height: '100vh', 
      width:'100vh'
    }}>
      <div style={{ 
        position: 'relative', 
        width: '50%', 
        height: '50%', 
        borderRadius: '50%',
      }}>
        <MachinePieChart data={data} state1={remoteMachines} />
      </div>
    </div>
  );
};
export default App;
