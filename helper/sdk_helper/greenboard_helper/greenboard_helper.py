from helper.sdk_helper.sdk_helper import SDKHelper
class GreenboardSDKHelper(SDKHelper):
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(GreenboardSDKHelper, cls).__new__(cls)
        return cls.instance

    def __init__(self) -> None:
        raise NotImplementedError("The helper has to be implemented")