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


def send_message_by_tag(content, tag=None):
    tag = tag.upper()
    bindings = get_bindings()
    for b in bindings:
        if tag is not None:
            tags = [x.upper() for x in b.tags]
            if tag not in tags:
                continue
        number = b.address
        send_message(number, content)
