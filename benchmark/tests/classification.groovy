import org.apache.jmeter.samplers.SampleResult
import org.apache.jmeter.threads.JMeterVariables

def fixtures = [
    ['200', '{"success":true,"data":660000000000000001}', 'accepted', '660000000000000001'],
    ['200', '{"success":false,"errorMsg":"库存不足"}', 'sold_out', ''],
    ['200', '{"success":false,"errorMsg":"不能重复下单"}', 'duplicate', ''],
    ['429', '{"success":false}', 'rate_limited', ''],
    ['202', '{"success":true,"data":{"orderId":660000000000000002,"status":"UNKNOWN"}}', 'pending', '660000000000000002'],
    ['202', 'invalid-json', 'pending', ''],
    ['503', '{"success":false}', 'server_error', ''],
    ['200', '{"success":false,"errorMsg":"服务器异常"}', 'unexpected', ''],
    ['200', 'not-json', 'unexpected', ''],
    ['Non HTTP response code: java.net.SocketTimeoutException', '', 'transport_error', '']
]
fixtures.each { fixture ->
    def prev = new SampleResult()
    prev.setResponseCode(fixture[0])
    prev.setResponseData(fixture[1], 'UTF-8')
    prev.setDataEncoding('UTF-8')
    def vars = new JMeterVariables()
    new GroovyShell(new Binding([prev:prev, vars:vars])).evaluate(new File('benchmark/jmeter/classify.groovy'))
    assert vars.get('category') == fixture[2]
    assert vars.get('orderId') == fixture[3]
    assert prev.isSuccessful() == (fixture[2] == 'accepted')
}
println "${fixtures.size()} response fixtures passed; 64-bit order IDs preserved."
