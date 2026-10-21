def giveup_on_http_4xx_except_429(error):
    response = error.response
    if response is None:
        return False
    return not (response.status_code == 429 or
                response.status_code >= 500)
