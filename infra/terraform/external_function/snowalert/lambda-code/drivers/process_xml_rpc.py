import xmlrpc.client


def process_row(url, method_name, **kwargs):
    method = getattr(xmlrpc.client.ServerProxy(url), method_name)
    return method(**kwargs)
