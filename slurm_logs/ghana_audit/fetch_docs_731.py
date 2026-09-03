"""Fetch the GhanaLSS documentation blobs needed as codebooks for GH #731/#705
through data_access.get_data_file() (lock-free), printing the local paths."""
import os, sys
def main():
    from lsms_library.paths import countries_root
    from lsms_library.data_access import get_data_file
    root = countries_root(); assert 'wt-glss-731-705' in str(root), root
    want = [
        ('1991-92', 'Documentation/pdf/G3QPartA.pdf'),
        ('1991-92', 'Documentation/pdf/G3Intman.pdf'),
        ('1991-92', 'Documentation/GHA_1991_GLSS_Household_Questionnaire_EN.pdf'),
        ('2005-06', 'Documentation/G5QPartA.pdf'),
        ('2012-13', 'Documentation/MANUALS/GLSS6 CODEBOOK.pdf'),
        ('2012-13', 'Documentation/QUESTIONNAIRES/GLSS6 Part A Questionnaire.pdf'),
        ('2012-13', 'Documentation/MANUALS/GLSS6 INTERVIEWERS MANUAL.pdf'),
        ('2016-17', 'Documentation/GLSS7_MODULE A_30_9_16_for print.docx'),
        ('1987-88', 'Documentation/questionnaire/GHA_1987_GLSS_Questionnaire_EN.pdf'),
        ('1987-88', 'Documentation/technical document/GHA_1987_GLSS_Interviewer_EN.pdf'),
        ('1988-89', 'Documentation/questionnaire/GHA_1988_GLSS_Household_Questionnaire_EN.pdf'),
        ('1988-89', 'Documentation/technical document/GHA_1988_GLSS_Interviewer_EN.pdf'),
    ]
    for w, rel in want:
        p = root / 'GhanaLSS' / w / rel
        try:
            out = get_data_file(str(p))
            print(f"OK   {w} {rel} -> {out} ({os.path.getsize(out)} bytes)")
        except Exception as e:
            print(f"FAIL {w} {rel}: {type(e).__name__}: {e}")
if __name__ == '__main__':
    main()
