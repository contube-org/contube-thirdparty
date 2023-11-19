plugins {
    id("java")
    `maven-publish`
    checkstyle
}

group = "com.zikeyang.contube"
version = "1.0-SNAPSHOT"

repositories {
    mavenLocal()
    mavenCentral()
}

checkstyle {
    toolVersion = "10.12.4"
    configFile = file("${project.rootDir}/checkstyle/checkstyle.xml")
    isShowViolations = true
}

dependencies {
    var pulsarVersion = "3.0.1"
    var contubeVersion = "1.0-SNAPSHOT"
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    implementation("com.zikeyang.contube:contube-common:$contubeVersion")
    implementation("org.apache.pulsar:pulsar-functions-utils:$pulsarVersion")
    implementation("org.apache.pulsar:pulsar-common:$pulsarVersion")
    implementation("org.apache.pulsar:pulsar-client-original:$pulsarVersion")
    implementation("org.apache.bookkeeper:circe-checksum:4.16.2")
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("org.slf4j:slf4j-simple:2.0.9")
    implementation("com.google.protobuf:protobuf-java:3.6.1")
    runtimeOnly("com.zikeyang.contube:contube-runtime:$contubeVersion")

    compileOnly("org.projectlombok:lombok:1.18.24")
    annotationProcessor("org.projectlombok:lombok:1.18.24")
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<Copy>("copyDependencies") {
    from(configurations.runtimeClasspath)
    into("${project.rootDir}/lib")
}
tasks.named("jar") {
    dependsOn("copyDependencies")
}

publishing {
    publications {
        create<MavenPublication>("ConTube") {
            from(components["java"])
            artifactId = project.name
            version = project.version.toString()
        }
    }
}
