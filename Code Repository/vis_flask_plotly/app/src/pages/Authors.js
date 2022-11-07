import './Authors.css';
import Plot from 'react-plotly.js';

const Authors = ({data}) => {

  console.log('############', data,'############')

  return (
    <Plot
      data={[{
        type: 'bar',
        x: data['publication_date'],
        y: data['no_publications']
      }]}
      layout={{width: 640, height: 480, title: 'Publications over time'}}
      revision={(Math.ceil(Math.random()*100000))}
    />
  )
}

export default Authors;