# Compatibility-Shim Migration Slice For `uppi/docs/`

Цей документ фіксує поточний Sprint 3 migration slice для production code, який
історично лежав у `uppi/docs/`.

Мета цього кроку:

- дати production code більш коректний runtime-home;
- зберегти old import path working;
- не робити mass relocation cleanup;
- не змінювати document-generation behavior.

## Scope цього slice

У цьому PR мігрується лише реально активний production helper:

- old path:
  [../uppi/docs/attestazione_template_filler.py](../uppi/docs/attestazione_template_filler.py)
- new canonical home:
  [../uppi/services/attestazione_template_filler.py](../uppi/services/attestazione_template_filler.py)

Причина такого вузького scope:

- саме цей модуль реально використовується в production document-generation path
- baseline DOCX/pipeline tests already cover його behavior
- широкий перенос усього `uppi/docs/` підвищив би blast radius без додаткової користі

## Compatibility Contract

Current migration contract:

- new canonical implementation lives in `uppi/services/attestazione_template_filler.py`
- old import path `uppi.docs.attestazione_template_filler` still works
- old path is now thin compatibility shim / re-export
- behavior, output shape і placeholder semantics не змінюються

## What Was Not Moved In This PR

Свідомо deferred:

- `uppi/docs/convert_pdf_to_marcdown.py`
  - це не active production runtime dependency для pipeline
- `uppi/docs/visura_pdf_parser.py`
  - він already є compatibility shim і не потребував нового migration slice тут
- будь-який mass cleanup інших historical files у `uppi/docs/`

## Follow-Up Scope

Later safe cleanup може включати:

- migration remaining production-adjacent helpers one-by-one
- final removal of old shim paths only after:
  - import surface audit
  - green tests
  - окремий low-risk PR

У цьому PR final deletion of old import path навмисно не робиться.

## Related Documents

- [./document_generation.md](./document_generation.md)
- [./current_architecture.md](./current_architecture.md)
