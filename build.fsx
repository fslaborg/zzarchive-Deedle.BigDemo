#r @"packages/FAKE/tools/FakeLib.dll"
open Fake

Target "Clean" (fun _ ->
    CleanDirs ["src/Deedle.BigSources/bin"]
)

Target "Build" (fun _ ->
    !! "src/Deedle.BigSources/Deedle.BigSources.fsproj"
    |> MSBuildRelease "" "Rebuild"
    |> ignore
)

"Clean"
  ==> "Build"

RunTargetOrDefault "Build"
