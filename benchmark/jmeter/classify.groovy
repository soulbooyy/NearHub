import groovy.json.JsonSlurper

// No token, response body or request header is written to JTL.
def category = 'unexpected'
def orderId = ''
def code = prev.getResponseCode()
try {
    def body = new JsonSlurper().parseText(prev.getResponseDataAsString())
    if (code == '202') {
        category = 'pending'
        if (body.data instanceof Map && body.data.orderId != null) orderId = body.data.orderId.toString()
    } else if (code == '429') {
        category = 'rate_limited'
    } else if (code == '200' && body.success == true && body.data instanceof Number && body.data.longValue() > 0) {
        category = 'accepted'
        orderId = body.data.toString()
    } else if (code == '200' && body.success == false && body.errorMsg == '库存不足') {
        category = 'sold_out'
    } else if (code == '200' && body.success == false && body.errorMsg == '不能重复下单') {
        category = 'duplicate'
    }
} catch (Exception ignored) { }
if (code == '202') category = 'pending'
else if (code == '429') category = 'rate_limited'
else if (code ==~ /5\d\d/) category = 'server_error'
else if (!(code ==~ /\d{3}/)) category = 'transport_error'
vars.put('category', category)
vars.put('orderId', orderId)
// JMeter HTML error% means non-accepted; custom summary separates expected rejection from anomalies.
prev.setSuccessful(category == 'accepted')
