plugins {
    id("java")
    `maven-publish`
    checkstyle
}

group = "com.zikeyang.contube"
version = "1.0-SNAPSHOT"
var contubeVersion = "1.0-SNAPSHOT"
var pulsarVersion = "3.0.1"

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "checkstyle")
    apply(plugin = "maven-publish")

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
        implementation("com.zikeyang.contube:contube-common:$contubeVersion")

        testImplementation(platform("org.junit:junit-bom:5.9.1"))
        testImplementation("org.junit.jupiter:junit-jupiter")

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
            create<MavenPublication>("ConTube-Pulsar") {
                from(components["java"])
                artifactId = project.name
                version = project.version.toString()
            }
        }
    }
}

project(":contube-pulsar") {
    dependencies {
        implementation("org.apache.pulsar:pulsar-functions-utils:$pulsarVersion")
        implementation("org.apache.pulsar:pulsar-common:$pulsarVersion")
        implementation("org.apache.pulsar:pulsar-client-original:$pulsarVersion")
        implementation("org.apache.bookkeeper:circe-checksum:4.16.2")
    }
}

project(":contube-pulsar-runtime") {
    dependencies {
        implementation(project(":contube-pulsar"))
        implementation("com.zikeyang.contube:contube-runtime:$contubeVersion")
        runtimeOnly("org.apache.logging.log4j:log4j-core")
    }
}
