import tap_tester.imports

""" Dynamically loads subtests based on this file's location and module name. """
tap_tester.imports.import_subtests(__file__, __name__)
