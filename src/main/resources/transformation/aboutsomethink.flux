default mode = "baseline";

"/data/pubhub/export/alma/prod/" + mode + ".xml.bgzf"
|open-file(decompressConcatenated="true")
|decode-xml
|handle-marcxml
|fix(FLUX_DIR + "aboutsomethink.fix", *)
|encode-marcxml
|write(member + "-" + mode + ".xml.gz")
;
