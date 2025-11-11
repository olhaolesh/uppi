"""
Selectors used by the UppiSpider for web scraping.
"""
class UppiSelectors:
    """Selectors for the UppiSpider."""
    # Login form selectors
    FISCOLINE_TAB = 'ul > li > a[href="#tab-4"]'
    USERNAME_FIELD = '#username-fo-ent'
    PASSWORD_FIELD = '#password-fo-ent-1'
    PIN_FIELD = '#pin-fo-ent'
    ACCEDI_BUTON = 'button.btn-primary[type="submit"]'
    # Profile selector to confirm login
    PROFILE_INFO ='#user-info'
    TUOI_PREFERITI_SECTION = 'label:has-text("I tuoi preferiti")'
    # SERVIZI SISTER selectors
    CONFERMA_BUTTON = 'input[value="Conferma"]'
    CONSULTAZIONI_CERTIFACAZIONI = '[data-active="Consultazioni e Certificazioni"]'
    VISURE_CATASTALI = 'li[data-active="Visure catastali"]'
    CONFERMA_LETTURA = 'a:has-text("Conferma Lettura")'
    SELECT_UFFICIO = 'select[name="listacom"]'
    APLICA_BUTTON = 'input[value="Applica"]'
    SELECT_CATASTO = 'select[name="tipoCatasto"]'
    SELECT_COMUNE = 'select[name="comuneCat"]'
    CODICE_FISCALE_RADIO = 'input[name="selDatiAna"][value="CF_PF"]'
    CODICE_FISCALE_FIELD = "#cf"
    RICERCA_BUTTON = 'input[name="ricerca"]'
    SELECT_OMONIMI = 'input[name="omonimoSelezionato"]'
    IMOBILI_BUTTON = 'input[name="immobili"]'
    ELENCO_IMOBILE = 'table > tbody:nth-child(2) > tr:nth-child(1) > td > input'
    VISURA_PER_IMOBILE_BUTTON = 'input[name="visuraImm"]'

    # CAPTCHA selector
    IMG_CAPTCHA = 'span > #imgCaptcha'
    CAPTCHA_FIELD = '#inCaptchaChars'

    INOLTRA_BUTTON = 'input[name="inoltra"]'
    # Open document
    APRI_BUTTON = 'input[value="Apri"]'