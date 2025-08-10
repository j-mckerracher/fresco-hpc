import React from "react";
import Image from "next/image";
import Link from "next/link";
import Header from "@/components/Header";
import Footer from "@/components/Footer";

const About: React.FC = () => {
  return (
    <div className="bg-black min-h-screen flex flex-col">
      <Header />
      <div className="mx-20 mt-8 p-2 bg-zinc-800">
        <h1 className="mb-8 text-center text-3xl font-bold text-yellow-400 md:text-4xl">
          Open repository and analysis of system usage data
        </h1>

        <div className="p-2 text-center">
          <div className="flex flex-col items-center justify-between gap-4">
            <Image
              src={"/assets/fresco-poster-header.jpg"}
              width={0}
              height={0}
              sizes="100vw"
              className="w-full h-full"
              alt={"fresco header"}
            />
            <Link
              href="/assets/fresco-poster.pdf"
              className="text-blue-500 text-2xl"
            >
              Project poster from November 2023
            </Link>
          </div>

          <div className="text-zinc-200 p-6 text-lg gap-4 flex flex-col text-justify">
            <p>
              Dependability has become a necessary requisite property for many
              of the computer systems that surround us or work behind the scenes
              to support our personal and professional lives. Heroic progress
              has been made by computer systems researchers and practitioners
              working together to build and deploy dependable systems. However,
              an overwhelming majority of this work is not based on real
              publicly available failure data. As a result, results in small lab
              settings are sometime disproved years later, many avenues of
              productive work in dependable system design are closed to most
              researchers, and conversely, some unproductive work gets done
              based on faulty assumptions about the way real systems fail.
              Unfortunately, there does not exist any open system usage and
              failure data repository today for any recent computing
              infrastructure that is large enough, diverse enough, and with
              enough information about the infrastructure and the applications
              that run on them. We are addressing this pressing need that has
              been voiced repeatedly by computer systems researchers from
              various sub-domains.
            </p>
            <p>
              The project is collecting, curating, and presenting public failure
              data of large-scale computing systems, in a repository called
              FRESCO. Our initial sources are Purdue, U of Illinois at
              Urbana-Champaign, and U of Texas at Austin. The data sets comprise
              static and dynamic information about system usage and the
              workloads, and failure information, for both planned and unplanned
              outages. We are performing data analytics on these datasets to
              answer various questions, such as: (1) How do jobs utilize cluster
              resources in a university centrally managed cluster? (2) How do
              users use or do not use the options to share resources on a node?
              (3) How often are the typical resources (compute, memory, local
              IO, remote IO, networking) overstretched by the demand and does
              such contention affect the failure rates of jobs? (4) Can users
              estimate the time their jobs will need on the cluster?
            </p>

            <div>
              <h2 className="text-center text-3xl font-bold text-yellow-400">
                Further Reading
              </h2>
              <ol className="list-decimal">
                <li>
                  FRESCO: Open Source Data Repository for Computational Usage
                  and Failures. At:{" "}
                  <a
                    className="text-blue-500"
                    href="https://diagrid.org/resources/1093"
                  >
                    https://diagrid.org/resources/1093
                  </a>
                </li>
                <li>
                  Subrata Mitra, Suhas Raveesh Javagal, Amiya K. Maji (ITaP),
                  Todd Gamblin (LLNL), Adam Moody (LLNL), Stephen Harrell
                  (ITaP), and Saurabh Bagchi, “A Study of Failures in Community
                  Clusters: The Case of Conte,” At the 7th IEEE International
                  Workshop on Program Debugging, co-located with ISSRE, pp. 1-8,
                  Oct 23-27, 2016, Ottawa, Canada.
                </li>
                <li>
                  Amiya Maji, Subrata Mitra, Bowen Zhou, Saurabh Bagchi, and
                  Akshat Verma (IBM Research), “Mitigating Interference in Cloud
                  Services by Middleware Reconfiguration,” At the 15th Annual
                  ACM/IFIP/USENIX Middleware conference, pp. 277-288, December
                  8-12, 2014, Bordeaux, France. (Acceptance rate: 27/144 =
                  18.8%)
                </li>
              </ol>
            </div>
          </div>
        </div>
      </div>
      <Footer />
    </div>
  );
};

export default About;
