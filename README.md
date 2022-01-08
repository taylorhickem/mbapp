# mbapp
utility for storage management. uses python module and google sheets for reports

### classes

* MBEngine

### MemoryEngine

Create instance

`mb = mbapp.MBEngine()
`

Create profile record

`mb.record_profile()
`

Load profile

`mb.load_profile()
`

Get profile

`mb_profile = mb.get_profile(
        asset, profile_date=yyyymmdd
)
`

