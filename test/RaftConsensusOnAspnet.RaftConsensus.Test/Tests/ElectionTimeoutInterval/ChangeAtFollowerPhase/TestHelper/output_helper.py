LOGGING_PREFIX = "<LOGGING>"
TEST_MESSAGE_PREFIX = "<TEST>"


def log(message):
    print(f"{LOGGING_PREFIX}{message}")

def test_output(message):
    print(f"{TEST_MESSAGE_PREFIX}{message}")

def extract_outputs(outputs):
    logs , test_messages = [] , []
    for output in outputs.split("\n"):
        if len(output) >= len(LOGGING_PREFIX) and output[0:len(LOGGING_PREFIX)] == LOGGING_PREFIX:
            logs.append(output[len(LOGGING_PREFIX):])
        elif len(output) >= len(TEST_MESSAGE_PREFIX) and output[0:len(TEST_MESSAGE_PREFIX)] == TEST_MESSAGE_PREFIX:
            test_messages.append(output[len(TEST_MESSAGE_PREFIX):])
    return logs, test_messages