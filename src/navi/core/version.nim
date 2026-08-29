## Single source of truth for navi's version string.
##
## Used for the default `User-Agent` header. Kept in sync with the `version`
## field in navi.nimble (the release source of truth); a build via nimble
## overrides it with the packaged version through `-d:naviVersion=<v>`.

const naviVersion* {.strdefine.} = "0.8.0"
