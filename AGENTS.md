# Repository Notes

- For iQuant/Xuntou strategy files that declare `#coding:gbk`, keep the file bytes in normal UTF-8 unless the user explicitly asks otherwise. Xuntou handles the conversion on import; do not rewrite these files as GBK just because the coding header says `gbk`.
