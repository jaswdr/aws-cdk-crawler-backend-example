from random_user_agent.params import SoftwareName, OperatingSystem
from random_user_agent.user_agent import UserAgent


software_names = [
    SoftwareName.CHROME.value,
    SoftwareName.FIREFOX.value]
operating_systems = [
    OperatingSystem.WINDOWS.value,
    OperatingSystem.LINUX.value,
    OperatingSystem.MACOS.value]
user_agent_rotator = UserAgent(
    software_names=software_names,
    operating_systems=operating_systems,
    limit=100)


def get_user_agent():
    return user_agent_rotator.get_random_user_agent()
