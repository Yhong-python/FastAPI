class Result(object):

    @staticmethod
    def success(code=200, message='成功', data=None, **kwargs):
        return Result.get_result(
            success=True,
            code=code,
            data=data,
            message=message,
            **kwargs
        )

    @staticmethod
    def fail(code=500, message='系统错误', **kwargs):
        return Result.get_result(
            success=False,
            code=code,
            message=message,
            **kwargs
        )

    @staticmethod
    def get_result(success, code, message, data=None, **kwargs):
        result = {
            'success': success,
            'code': code
        }
        if not ''.__eq__(message):
            result['message'] = message
        if not {}.__eq__(kwargs):
            if "detail" in kwargs:
                result['detail'] = kwargs['detail']
            else:
                result['data'] = kwargs
        if data is not None:
            result['data'] = data
        return result
