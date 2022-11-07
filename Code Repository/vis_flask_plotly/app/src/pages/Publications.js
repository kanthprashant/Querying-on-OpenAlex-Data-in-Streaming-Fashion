import './Publications.css';
import Plot from 'react-plotly.js';

const Publications = ({data}) => {

  console.log('############', data,'############')

  return (
    <Plot
      data={[{
        type: 'bar',
        x: data['publication_date'],
        y: data['no_publications']
      }]}
      layout={{width: 640*4/3, height: 480*4/3, title: 'Publications over time'}}
      revision={(Math.ceil(Math.random()*100000))}
    />
  )
}

export default Publications;
