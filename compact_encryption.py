m __future__ import annotations


class CompactEncryptor:
    _CHARSET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

    def __init__(self, secret_key: int = 42) -> None:
        self._secret_key = secret_key

    def encrypt(self, number: int) -> str:
        if number < 0 or number > 9_999_999:
            raise ValueError("Zahl muss zwischen 0 und 9999999 liegen")

        encrypted_num = (number ^ self._secret_key) & 0xFFFFFFFF
        encrypted_num = ((encrypted_num << 7) | (encrypted_num >> (24 - 7))) & 0xFFFFFF
        encrypted_num = (encrypted_num * 16777619) & 0xFFFFFFFF

        result = self._to_base62(encrypted_num)
        return result.rjust(7, "0")

    def decrypt(self, encrypted_text: str) -> int:
        if not encrypted_text:
            raise ValueError("Verschluesselter Text darf nicht leer sein")

        encrypted_num = self._from_base62(encrypted_text)
        encrypted_num = (encrypted_num * 3618502749) & 0xFFFFFFFF
        encrypted_num = ((encrypted_num >> 7) | (encrypted_num << (24 - 7))) & 0xFFFFFF
        original_num = encrypted_num ^ (self._secret_key & 0xFFFFFFFF)
        return int(original_num)

    def _to_base62(self, num: int) -> str:
        if num == 0:
            return "0"

        base_value = len(self._CHARSET)
        out = []
        while num > 0:
            out.append(self._CHARSET[num % base_value])
            num //= base_value
        return "".join(reversed(out))

    def _from_base62(self, text: str) -> int:
        base_value = len(self._CHARSET)
        num = 0
        for char in text:
            idx = self._CHARSET.find(char)
            if idx == -1:
                raise ValueError(f"Ungueltiges Zeichen: {char}")
            num = num * base_value + idx
        return num
