class GreenboardSDKHelper:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(GreenboardSDKHelper, cls).__new__(cls)
        return cls.instance

    def __init__(self) -> None:
        raise NotImplementedError("The helper has to be implemented")