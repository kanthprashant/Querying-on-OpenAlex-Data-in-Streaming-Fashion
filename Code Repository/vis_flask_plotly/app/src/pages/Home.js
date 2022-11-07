import './Home.css';

const Home = () => {

  return (
    <>
      <h3>What is OpenAlex?</h3>
      <ul>
        <li>OpenAlex is an index of hundreds of millions of interconnected entities across the global research system</li>
        <li>OpenAlex dataset consists of information about scholarly documents like journal articles, books, datasets, and theses</li>
        <li>Information about new works are collected from many sources, including Crossref, PubMed, institutional and discipline- specific repositories (eg, arXiv)</li>
      </ul>
      
      <h3>Project Goal</h3>
      <p>Our project aims at efficiently fetching this required information in the form of database queries in a streaming fashion.</p>  
      
      <h3>Dataset</h3>
      <ul>
        <li>OpenAlex indexes about 242M works, with about 50,000 added daily. This amounts to a total size of 300GB (approx.)</li>
        <li>Hence, for feasibility and due to storage limitations, we get the works from the past 5 years that are related to Computer Science</li>
        <li>This reduces the number of works to 35M and the size to a little over 100GB (approx.)</li>
      </ul>
      
      <h3>Tools</h3>
      <ul>
      <li>Machine - iLab Cluster</li>      
        <li>Processing - Spark & Spark Streaming</li>      
        <li>Web App - Flask & React</li>
        <li>Plots - Plotly.js</li>
      </ul>
      
      <h3>Group Info</h3>
      <p>Group 4 - Arun Subbaiah, Prashant Kanth, Sandesh Mangalore</p>
    </>
  )
}

export default Home;