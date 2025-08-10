import Footer from "@/components/Footer";
import Header from "@/components/Header";
import Image from "next/image";
import Link from "next/link";

interface TeamMember {
  name: string;
  image: string;
  email: string;
  webpage: string;
}

export default function Team() {
  const team: TeamMember[] = [
    {
      name: "Saurabh Bagchi",
      image: "/assets/headshots/saurabhbagchi.jpg",
      email: "sbagchi@purdue.edu",
      webpage: "https://bagchi.github.io/",
    },
    {
      name: "Carol Song",
      image: "/assets/headshots/carolsong.png",
      email: "cxsong@purdue.edu",
      webpage: "https://web.ics.purdue.edu/~cxsong/purdue/Welcome.html",
    },
    {
      name: "Amiya Maji",
      image: "/assets/headshots/amiyamaji.jpg",
      email: "amaji@purdue.edu",
      webpage: "https://www.rcac.purdue.edu/about/staff/amaji",
    },
    {
      name: "Aryamaan Dhomne",
      image: "/assets/headshots/aryamaandhomne.jpg",
      email: "adhomne@purdue.edu",
      webpage: "https://github.com/arya1106",
    },
    {
      name: "Stephen Harrell",
      image: "/assets/headshots/StephenHarrell.jpg",
      email: "sharrell@tacc.utexas.edu",
      webpage:
        "https://tacc.utexas.edu/about/staff-directory/stephen-lien-harrell/",
    },
    {
      name: "Joshua McKerracher",
      image: "/assets/headshots/josh.png",
      email: "jmckerra@purdue.edu",
      webpage: "https://mckerracher.github.io/portfolio_2.0/",
    },
  ];

  return (
    <div className="min-h-screen bg-black flex flex-col">
      <Header />
      <div className="container mx-auto w-[90%]">
        <div className="grid grid-cols-1 gap-8 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
          {team.map((member) => (
            <div
              key={member.name}
              className="flex flex-col items-center space-y-4"
            >
              <div className="relative h-48 w-48 overflow-hidden rounded-full">
                <Image
                  src={member.image}
                  alt={member.name}
                  fill
                  className="object-cover"
                />
              </div>
              <h2 className="text-xl font-semibold text-yellow-500">
                {member.name}
              </h2>
              <div className="flex space-x-2">
                <Link
                  href={member.email}
                  className="px-4 py-2 bg-gray-700 text-white text-sm font-medium rounded hover:bg-gray-600 transition-colors"
                >
                  Email
                </Link>
                <Link
                  href={member.webpage}
                  className="px-4 py-2 bg-gray-700 text-white text-sm font-medium rounded hover:bg-gray-600 transition-colors"
                >
                  Webpage
                </Link>
              </div>
            </div>
          ))}
        </div>
        <Footer />
      </div>
    </div>
  );
}
