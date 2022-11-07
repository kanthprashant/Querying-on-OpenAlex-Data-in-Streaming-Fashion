import './Papers.css';
import Plot from 'react-plotly.js';

const Papers = ({data}) => {

  let traces;
  let cncpts = [
    'ARTIFICIAL INTELLIGENCE', 'COMPUTER VISION', 'MACHINE LEARNING', 
    'DISTRIBUTED COMPUTING', 'NATURAL LANGUAGE PROCESSING', 'DATA MINING'
  ]  
  console.log('This is happening1')
  if (Object.keys(data).length === 0) {
    console.log('This is happening2')
    traces = cncpts.map((cncpt, i) => ({
      type: 'scatter',
      y: [],
      x: [],
      xaxis: `x${i+1}`,
      yaxis: `y${i+1}`,
      orientation: 'h',
      name: cncpt   
    }))
  } else {
    traces = cncpts.map((cncpt, i) => ({
      type: 'bar',
      y: data[cncpt]['display_name'],
      x: data[cncpt]['cited_by_count'],
      xaxis: `x${i+1}`,
      yaxis: `y${i+1}`,
      orientation: 'h',
      name: cncpt 
    }))  
  }

  let layout = {  
    grid: {rows: 2, columns: 3, pattern: 'independent'},
    width: 640*4/3, height: 480*4/3, title: 'Prominent papers in selected domains',
    yaxis1: { showticklabels: false },
    yaxis2: { showticklabels: false },
    yaxis3: { showticklabels: false },
    yaxis4: { showticklabels: false },
    yaxis5: { showticklabels: false },
    yaxis6: { showticklabels: false },    
  };

  console.log('############', traces,'############')
  console.log('############', layout,'############')


  return (
    <Plot
      data={traces}
      layout={layout}
      revision={(Math.ceil(Math.random()*100000))}
    />
  )
}

export default Papers;