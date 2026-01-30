1. 符合送校:
    TypeError
    TypeError: not all arguments converted during string formatting

    Traceback (most recent call last)
    File "C:\Users\So\AppData\Local\Programs\Python\Python312\Lib\site-packages\flask\app.py", line 1536, in __call__
    return self.wsgi_app(environ, start_response)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    File "C:\Users\So\AppData\Local\Programs\Python\Python312\Lib\site-packages\flask\app.py", line 1514, in wsgi_app
    response = self.handle_exception(e)
            ^^^^^^^^^^^^^^^^^^^^^^^^
    File "C:\Users\So\AppData\Local\Programs\Python\Python312\Lib\site-packages\flask\app.py", line 1511, in wsgi_app
    response = self.full_dispatch_request()
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    File "C:\Users\So\AppData\Local\Programs\Python\Python312\Lib\site-packages\flask\app.py", line 919, in full_dispatch_request
    rv = self.handle_user_exception(e)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    File "C:\Users\So\AppData\Local\Programs\Python\Python312\Lib\site-packages\flask\app.py", line 917, in full_dispatch_request
    rv = self.dispatch_request()
        ^^^^^^^^^^^^^^^^^^^^^^^
    File "C:\Users\So\AppData\Local\Programs\Python\Python312\Lib\site-packages\flask\app.py", line 902, in dispatch_request
    return self.ensure_sync(self.view_functions[rule.endpoint])(**view_args)  # type: ignore[no-any-return]
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    File "C:\Users\So\Desktop\doc\yfjcgkzx\hqzcsj\routes\zfba_wcnr_jqaj_routes.py", line 118, in detail_page
    rows, truncated = fetch_detail(
                    
    File "C:\Users\So\Desktop\doc\yfjcgkzx\hqzcsj\service\zfba_wcnr_jqaj_service.py", line 319, in fetch_detail
    return zfba_wcnr_jqaj_dao.fetch_detail_rows(
        
    File "C:\Users\So\Desktop\doc\yfjcgkzx\hqzcsj\dao\zfba_wcnr_jqaj_dao.py", line 827, in fetch_detail_rows
    return _exec(cur, q, params8)
        ^^^^^^^^^^^^^^^^^^^^^^
    File "C:\Users\So\Desktop\doc\yfjcgkzx\hqzcsj\dao\zfba_wcnr_jqaj_dao.py", line 459, in _exec
    cur.execute(q, params)
    ^^^^^^^^^^^^^^^^^^^^^^
    TypeError: not all arguments converted during string formatting
    The debugger caught an exception in your WSGI application. You can now look at the traceback which led to the error.
    To switch between the interactive traceback and the plaintext one, you can click on the "Traceback" headline. From the text traceback you can also create a paste of it.

    Brought to you by DON'T PANIC, your friendly Werkzeug powered traceback interprete
2. 
