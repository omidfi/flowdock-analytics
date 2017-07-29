var cluster = require('cluster')
var _ = require('lodash')
require('dotenv').config()

// instructions in ./env-sample file
const flowsToDownload = process.env.FLOWS_TO_DOWNLOAD
  .replace(/ /gm, '')
  .split(',')

var numWorkers = require('os').cpus().length
let chunkSize = numWorkers / Math.floor(flowsToDownload.length / numWorkers) + 1
var chunkedFlowsToDownload = _.chunk(flowsToDownload, chunkSize)

if (cluster.isMaster) {
  console.log('Master cluster setting up ' + numWorkers + ' workers...')

  for (var i = 0; i < numWorkers; i++) {
    cluster.fork()
  }

  cluster.on('online', function (worker) {
    console.log('Worker ' + worker.process.pid + ' is online')
  })

  cluster.on('exit', function (worker, code, signal) {
    console.log(
      'Worker ' +
        worker.process.pid +
        ' died with code: ' +
        code +
        ', and signal: ' +
        signal
    )
    for (var id in cluster.workers) {
      cluster.workers[id].kill()
    }
    // exit the master process
    process.exit(0)
  })
} else {
  const {
    getMessagesCount,
    downloadFlowDockMessages
  } = require('./messages.js')
  const { getUsers } = require('./users.js')
  const {
    createElasticsearchIndex,
    getLatestMessageIdInFlow
  } = require('./elasticsearch.js')

  console.log(
    'Hello! starting to download and index messages',
    flowsToDownload.length,
    'flows'
  )
  console.log(
    'Total message numbers are just estimates.',
    '\n Only new messages after the last indexing will be downloaded'
  )

  async function init () {
    await createElasticsearchIndex()
    let users = await getUsers()
    let downloadOneFlow = async flowName => {
      try {
        await downloadFlowDockMessages(
          flowName,
          await getLatestMessageIdInFlow(flowName),
          [],
          users,
          await getMessagesCount(flowName)
        )
      } catch (error) {
        console.log(error)
      }
    }

    chunkedFlowsToDownload[cluster.worker.id].forEach(downloadOneFlow)
  }

  init()
}
