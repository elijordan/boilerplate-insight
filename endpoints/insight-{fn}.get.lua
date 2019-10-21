--#ENDPOINT GET /insight/{fn}
request.function_id = request.parameters.fn
return require('insight').infoInsight(request)
