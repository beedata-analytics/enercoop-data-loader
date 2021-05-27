import base64
import settings

# security for PRM
def encode(clear):
    key = settings.ANONYMIZE_KEY
    enc = []
    for i in range(len(clear)):
        key_c = key[i % len(key)]
        enc_c = (ord(clear[i]) + ord(key_c)) % 256
        enc.append(enc_c)
    return base64.urlsafe_b64encode(bytes(enc)).decode("utf-8")

def decode(enc):
    key = settings.ANONYMIZE_KEY
    dec = []
    enc = base64.urlsafe_b64decode(enc.encode("utf-8"))
    for i in range(len(enc)):
        key_c = key[i % len(key)]
        dec_c = chr((256 + enc[i] - ord(key_c)) % 256)
        dec.append(dec_c)
    return "".join(dec)