from twilio.rest import Client

from idem_settings import twilio_number, twilio_sid, twilio_token, notify_sid


def send_message(to, content=""):
    client = Client(twilio_sid, twilio_token)
    if not to.startswith("+"):
        to = "+" + to
    client.messages.create(to=to, from_=twilio_number, body=content)


def get_bindings():
    client = Client(twilio_sid, twilio_token)
    notify = client.notify.services(notify_sid)
    bindings = notify.bindings.list()
    return bindings


def broadcast_message_by_tag(content, tag=None):
    bindings = get_bindings()
    results = []
    for b in bindings:
        result = send_to_user_by_tag(content, b, tag)
        print b.address, str(result)
        results.append(result)
    return results


def send_to_user_by_tag(content, binding, tag=None):
    if tag is not None:
        tag = tag.upper()
        user_tags = [x.upper() for x in binding.tags]
        if tag not in user_tags:
            continue
    number = binding.address
    result = send_message(number, content)
    return result
