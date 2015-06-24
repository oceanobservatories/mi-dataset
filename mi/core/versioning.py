
def version(version_number):
    """Wrapper to add version name to parser.
    Placing the version number on the parse function seems to make
    more sense than adding __version__ to the code and relying on that.
   """
    def decorate(f):
        setattr(f, 'version', version_number)
        return f
    return decorate
