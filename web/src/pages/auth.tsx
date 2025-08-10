import Header from "@/components/Header";
import OAuthBox from "@/components/auth/oauth-box";

const Auth = () => {
  return (
    <div className="bg-black position min-h-screen flex flex-col">
      <Header />
      <OAuthBox />
    </div>
  );
};

export default Auth;
