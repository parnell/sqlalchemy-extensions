class NoLogicalKeyException(Exception):
    """Raise when a class has no defined logical keys"""

    def __init__(self, cls_str: str, message: str = None, *args):
        """Init for NoLogicalKeyException

        Args:
            cls_str (str): The name of the class that needs a logical key
            message (str, optional): Optional message. Defaults to None.
        """
        self.cls_str = cls_str
        self.add_note(
            f"Error: '{cls_str}' has no defined logical keys.  "
            "Class must contain at least one column with logical_key=True"
        )
        super().__init__(message, *args)
