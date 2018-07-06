import Snowflake from 'node-snowflake';

const worker = new Snowflake.Worker({ datacenterId: 0, workerId: 0 })

export default worker;