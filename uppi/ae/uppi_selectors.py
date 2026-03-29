"""CSS- і text-selector-и, які використовує browser-critical flow павука UPPI."""

class UppiSelectors:
    """Збирає selector-и AE / SISTER в одному місці без зміни їхнього порядку."""

    # Селектори форми входу
    FISCOLINE_TAB = 'ul > li > a[href="#tab-4"]'
    USERNAME_FIELD = '#username-fo-ent'
    PASSWORD_FIELD = '#password-fo-ent-1'
    PIN_FIELD = '#pin-fo-ent'
    ACCEDI_BUTON = 'button.btn-primary[type="submit"]'

    # Селектори профілю
    PROFILE_INFO ='#user-info'
    ESCI_SISTER_BUTTON = 'a:has-text("Esci")'

    # Вибір сервісу SISTER
    TUOI_PREFERITI_SECTION = 'label:has-text("I tuoi preferiti")'
    VAI_AL_SERVIZIO_BUTTON = 'a[href*="ret2sister"]'

    # Домашня сторінка SISTER
    CONFERMA_BUTTON = 'input[value="Conferma"]'
    CONSULTAZIONI_CERTIFACAZIONI = '[data-active="Consultazioni e Certificazioni"]'
    VISURE_CATASTALI = 'li[data-active="Visure catastali"]'

    # Сторінка Visure catastali
    CONFERMA_LETTURA = 'a:has-text("Conferma Lettura")'
    SELECT_UFFICIO = 'select[name="listacom"]'
    APLICA_BUTTON = 'input[value="Applica"]'

    # Пошук фізичної особи
    SELECT_CATASTO = 'select[name="tipoCatasto"]'
    SELECT_COMUNE = 'select[name="comuneCat"]'
    CODICE_FISCALE_RADIO = 'input[name="selDatiAna"][value="CF_PF"]'
    CODICE_FISCALE_FIELD = "#cf"
    RICERCA_BUTTON = 'input[name="ricerca"]'

    # Список омонімів
    SELECT_OMONIMI = 'input[name="omonimoSelezionato"]'
    IMOBILI_BUTTON = 'input[name="immobili"]'
    VISURA_PER_SOGGECTO_BUTTON = 'input[name="visura"]'
    
    # Список нерухомості за правами і частками
    SELECT_IMOBILE = 'table > tbody:nth-child(2) > tr:nth-child(1) > td > input'
    VISURA_PER_IMOBILE_BUTTON = 'input[name="visuraImm"]'

    # Кнопка «Visura per soggetto»
    INOLTRA_BUTTON = 'input[name="inoltra"]'

    # CAPTCHA
    IMG_CAPTCHA = 'span > #imgCaptcha'
    CAPTCHA_FIELD = '#inCaptchaChars'

    INOLTRA_BUTTON = 'input[name="inoltra"]'
    # Відкриття документа
    APRI_BUTTON = 'input[value="Apri"]'
    # Вихід із сесії
    LOGOUT_BUTTON = '#error-msg h2'
