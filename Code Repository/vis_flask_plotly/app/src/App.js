import { useState, useEffect} from 'react';
import { Layout, Menu, Button } from 'antd';
import Home from './pages/Home'
import Publications from './pages/Publications'
import Papers from './pages/Papers'
import Research from './pages/Research'
import Authors from './pages/Authors'
import './App.css'

const { Header, Content, Footer, Sider } = Layout;

const App = () => {

  console.log('App rendered')

  const server = "http://127.0.0.1:5000";
  let current_path = window.location.pathname

  let meta = {
    '/': {'id': 'home', 'title': 'Home', 'menu': 'Home'},
    '/publications': {'id': 'publications', 'title': 'Distribution of Publications over time', 'menu': 'Publications' },    
    '/papers': {'id': 'papers', 'title': 'Prominent Papers', 'menu': 'Prominent Papers' },    
    '/research': {'id': 'research', 'title': 'Growing Research Areas', 'menu': 'Research Areas' },    
    '/authors': {'id': 'authors', 'title': 'Prominent', 'menu': 'Authors' },        
  }

  let Component
  switch (current_path) {
    case '/': Component = Home; break
    case '/publications': Component = Publications; break;
    case '/papers': Component = Papers; break;  
    case '/research': Component = Research; break;
    case '/authors': Component = Authors; break;
  }

  const [data, setData] = useState({})
  const updateData = (newData) => {
    console.log('State *data* updated')
    setData(() => newData)
  }

  console.log(data)
  
  const startCompute = (e) => {
    let destination = `${server}${current_path}/start`
    console.log(`GET request sent to ${destination}`)
    fetch(destination).then(res => {
      console.log('Inside Fetch')
      return res.json()
    }

    ).then(
      res => {
        console.log('GET request response:', res)
      }
    ).catch(
      err => console.log(`GET request error: ${err}`)
    )
  }

  useEffect(() => {
    if (current_path !== '/') {
      const interval = setInterval(() => {
        let destination = `${server}${current_path}/data`
        console.log(`GET request sent to ${destination}`);
        fetch(destination).then(
          res => res.json()
        ).then(
          res => {
            console.log('GET request response:', res)
            updateData(res)
          }
        ).catch(
          err => console.log(`GET request error: ${err}`)
        )
      }, 5000);
      return () => clearInterval(interval);
    } 
  }, []);
  
  const ComputeButton = () => (
    <Button type='primary' size='small' onClick={startCompute}> 
      Start Computing 
    </Button>
  )

  return (
    <Layout>

      <Header className="header">
        <h1>OpenAlex Dashboard</h1>
      </Header>

      <Layout className="body">

        <Sider width={200} className="sider">
          <Menu 
            className="menu" mode="inline" 
            items={Object.keys(meta).map(key => ({'key': meta[key]['id'], 'label': meta[key]['menu']}))}
            // selectedKeys={['chart1']}
          />
        </Sider>

        <Content className="content white-bg">
          <Content className='title flex space-between'>
            <h2> {meta[current_path]['title']} </h2>
            {current_path !== '/' && <ComputeButton />}
          </Content>

          <Content className='plot'>
            <Component data={data} />
          </Content>
        </Content>
        
      </Layout>

      {/* <Footer className='footer'>
      Placeholder for footer
      </Footer> */}

    </Layout>
  )
};
export default App;