const pako = require('pako');

module.exports = {
  'r': pako.inflate(Buffer.from(compressed, 'base64'), { to: 'string' })
}
