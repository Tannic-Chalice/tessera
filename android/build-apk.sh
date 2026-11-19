#!/bin/bash
# Builds TESSERA-Navigator.apk without Gradle / Android Studio (Ubuntu: apt install android-sdk-platform-23
# aapt dalvik-exchange zipalign apksigner openjdk-17-jdk).  Usage: ./build-apk.sh
set -e
cd "$(dirname "$0")"
AJ=${ANDROID_JAR:-/usr/lib/android-sdk/platforms/android-23/android.jar}
rm -rf build && mkdir -p build/gen build/classes
aapt package -f -m -J build/gen -M AndroidManifest.xml -S res -I "$AJ"
javac -nowarn -source 8 -target 8 -bootclasspath "$AJ" -d build/classes \
  $(find src build/gen -name '*.java') 2>&1 | grep -v -e '^warning' -e 'Picked up' -e '^[0-9] warning' || true
dalvik-exchange --dex --output=build/classes.dex build/classes
aapt package -f -M AndroidManifest.xml -S res -A assets -I "$AJ" -F build/unsigned.apk
(cd build && aapt add -f unsigned.apk classes.dex >/dev/null)
[ -f tessera.keystore ] || keytool -genkeypair -keystore tessera.keystore -storepass tessera -keypass tessera \
  -alias tessera -keyalg RSA -keysize 2048 -validity 10000 -dname "CN=TESSERA Navigator, O=RVCE" 2>/dev/null
zipalign -f 4 build/unsigned.apk build/aligned.apk
apksigner sign --ks tessera.keystore --ks-pass pass:tessera --out TESSERA-Navigator.apk build/aligned.apk
apksigner verify TESSERA-Navigator.apk && ls -la TESSERA-Navigator.apk
