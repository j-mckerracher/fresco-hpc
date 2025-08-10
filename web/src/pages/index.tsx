import Hero from "@/components/landing/Hero";
import Header from "../components/Header";
import Image from "next/image";
import Footer from "@/components/Footer";
import { Analytics } from '@vercel/analytics/next';// import MainSection from "../components/MainSection";
// import Footer from "../components/Footer";

const Home = () => {
  return (
    <div className="bg-black min-h-screen flex flex-col">
      <Header />
      <Hero />
      {/* <section className="text-white py-16">
        <div className="flex justify-center">
          <div className="w-full max-w-4xl h-64 bg-gray-700 flex items-center justify-center">
            <p>Screenshot / GIF showing data analysis dashboard workflow</p>
          </div>
        </div>
      </section> */}
      <section className="text-white py-16">
        <div className="flex justify-center gap-5 mx-16">
          <Image
            src={"/assets/anvil.png"}
            width={0}
            height={0}
            sizes="100vw"
            style={{ width: "50%", height: "auto" }} // optional
            className="px-5"
            alt={""}
          />
          <div className="w-1/2 text-lg">
            <Analytics />
            <p>
              The FRESCO project is engaged in the systematic collection,
              curation, and presentation of public failure data pertinent to
              large-scale computing systems, all of which is consolidated in a
              repository named FRESCO. Originating from Purdue, U of Illinois at
              Urbana-Champaign, and U of Texas at Austin, the datasets
              encapsulate both static and dynamic information, encompassing
              system usage, workloads, and failure data, applicable to both
              planned and unplanned outages. Navigate through the subsequent
              link to delve into the data.
            </p>
            <p>
              In a broader context, the FRESCO project seeks to illuminate the
              intricacies of system failures and usage patterns within
              large-scale computing environments, thereby providing a rich data
              repository that stands to benefit researchers, technologists, and
              data scientists in navigating the complexities and challenges
              inherent in managing and maintaining robust computing
              infrastructures. This endeavor not only facilitates a deeper
              understanding of system failures but also propels further research
              and development in the realm of dependable computing systems.
            </p>
          </div>
        </div>
      </section>
      {/* <MainSection /> */}
      <Footer />
    </div>
  );
};

export default Home;
