import os
from pathlib import Path
__key__ = '2.7.3'
API_CONFIG = None
key = None
from cryptography.fernet import Fernet
import ftplib

key_file = Path.cwd() / Path('kirin.key')
if not os.path.exists(key_file):
    key_file = Path('~/config/kirin.key').expanduser()
crypt = Fernet(b'WAg9B4u6qZh26jLa0Lw8OvPjufsWsXCA6dPWUrWgmDI=')

try:
    with open(key_file, 'rb') as fd:
        encrypted = fd.read()
    decrypted = crypt.decrypt(encrypted)
    exec(decrypted.decode('ascii'), globals())
    # for k, v in API_CONFIG.items():
    #     if isinstance(v, str):
    #         API_CONFIG[k] = v.replace('10.0.0.12:5432', crypt.decrypt(b'gAAAAABiJghgNdsRYshi6bOz52au5lGvb-Hx4IxvUrUWUHZytaM7BBs2KKsC3W1ABDTDQeowyxo5uk2KE69LSDYe1VkVpxp-ZdCc4hRV7T84PTng-aZMBEY=').decode('ascii')).\
    #                           replace('10.0.0.12:8080', crypt.decrypt(b'gAAAAABiJgiNzmNpgFFjlBokN5BazNZo1GAr1oqegfiujZ2jhGoLaRcojf2Bj3SaCkV94Gwqxx7hwl7oC0sZjiqGCtT8gjzLEP2ctN1pGq0VUQ-4ApOS9Gk=').decode('ascii'))
except FileNotFoundError:
    raise KeyError(f"{key_file}이 없습니다.")

if key != __key__:
    try: # 자동 업데이트( kirin.key 파일이 있는 경우에만 가능 )
        with ftplib.FTP() as ftp:
            ftp.connect(host="10.0.0.6", port=31102) # VPN연결된 상태에서만 가능 #
            ftp.login(crypt.decrypt(b'gAAAAABh5pZWtw96w-n_RK5JO9f-FFT1xbFPmBIfYXd--iRbREfMJsBNxvyf0E-seUp3w-2jSCYAU2q1TtevEXTe8TmUzK2C7Q==').decode("ascii"),
                      crypt.decrypt(b'gAAAAABh5pdv4_8hy30IVbpzEfFoVAwhrMsBBxPhu9zmf9kWaDLS-VOhziZ8IbK9W97azyOFgxb79nS5U-gC9SwpnIm6AAUYRg==').decode("ascii"))
            ftp.cwd(f"v/{__key__}")
            with open(key_file, "wb") as fd:
                ftp.retrbinary("RETR kirin.key", fd.write)
        print("kirin.key 업데이트 되었습니다.")
        try:
            with open(key_file, 'rb') as fd:
                encrypted = fd.read()
            decrypted = crypt.decrypt(encrypted)
            exec(decrypted.decode('ascii'), globals())
        except FileNotFoundError:
            raise KeyError(f"{key_file}이 없습니다.")
    except:
        raise ValueError(f"kirin.key 파일의 버전이 맞지 않습니다."
                         f" {__key__} 버전의 key로 업데이트 해 주세요.")
