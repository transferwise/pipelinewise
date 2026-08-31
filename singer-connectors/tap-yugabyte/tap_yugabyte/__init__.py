import singer

LOGGER = singer.get_logger('tap_yugabyte')

def main():
    """
    main
    """
    try:
        main_impl()
    except Exception as exc:
        Logger.critical(exc)
        raise exc