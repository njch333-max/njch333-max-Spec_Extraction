# Private Evoca Real-PDF Fixtures

`tests/test_evoca_end_to_end.py` can run the real-PDF Evoca e2e suite when the
validated customer PDFs are available.

Do not commit customer PDFs to this repository. Put local copies in this ignored
directory, or point `EVOCA_E2E_PDF_DIR` at another private folder. The tests
skip cleanly when no private PDFs are available.

For a gating local or CI run, use:

```powershell
powershell -ExecutionPolicy Bypass -File tools\run_evoca_real_pdf_e2e.ps1
```

That runner requires all nine validated Evoca PDFs unless `-AllowSkip` is
explicitly passed for a non-gating smoke run.
