def normalize_element_key(grp: str, code: str) -> str:
    """
    Нормалізує ключ елемента до формату a1..d13.
    Підтримує два варіанти з БД:
      - grp='A', code='A1'  -> a1
      - grp='A', code='1'   -> a1
    """
    g = (grp or "").strip().lower()
    c = (code or "").strip().lower()
    if not g or not c:
        return ""

    # якщо code вже починається з a/b/c/d і далі цифра -> беремо як є
    if len(c) >= 2 and c[0] in ("a", "b", "c", "d") and c[1:].isdigit():
        return c

    # якщо code чисто "1", "12" -> додаємо grp
    if c.isdigit():
        return f"{g}{c}"

    # якщо code типу "A1" але з дивними пробілами -> пробуємо підчистити
    if c.startswith(g) and c[1:].isdigit():
        return c

    # fallback: як є (але це вже “нестандарт”)
    return c